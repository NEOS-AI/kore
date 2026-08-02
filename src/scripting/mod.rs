//! Lua script cache (SCRIPT LOAD / EVALSHA), Redis Functions library store,
//! and script runtime controls (`lua-time-limit`, SCRIPT KILL).
//!
//! Shared server-wide so SCRIPT LOAD / FUNCTION LOAD on one connection is visible to others.

use parking_lot::Mutex;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Default Redis-compatible `lua-time-limit` (milliseconds). `0` = unlimited.
pub const DEFAULT_LUA_TIME_LIMIT_MS: u64 = 5000;

/// SHA1 hex digest of a Lua script body (lowercase, Redis-compatible).
pub fn script_sha1(script: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(script.as_bytes());
    hex::encode(hasher.finalize())
}

/// In-memory SCRIPT LOAD cache keyed by lowercase SHA1 hex.
#[derive(Debug, Default)]
pub struct ScriptCache {
    scripts: Mutex<HashMap<String, String>>,
}

impl ScriptCache {
    pub fn new() -> Self {
        Self {
            scripts: Mutex::new(HashMap::new()),
        }
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Insert (or re-insert) a script; returns its SHA1 hex.
    pub fn load(&self, script: &str) -> String {
        let sha = script_sha1(script);
        self.scripts.lock().insert(sha.clone(), script.to_string());
        sha
    }

    /// Look up script source by SHA1 hex (case-insensitive).
    pub fn get(&self, sha: &str) -> Option<String> {
        let key = sha.to_ascii_lowercase();
        self.scripts.lock().get(&key).cloned()
    }

    /// SCRIPT EXISTS: 1 if present, 0 otherwise (order preserved).
    pub fn exists(&self, shas: &[String]) -> Vec<i64> {
        let map = self.scripts.lock();
        shas.iter()
            .map(|s| {
                if map.contains_key(&s.to_ascii_lowercase()) {
                    1
                } else {
                    0
                }
            })
            .collect()
    }

    /// SCRIPT FLUSH — drop all cached scripts.
    pub fn flush(&self) {
        self.scripts.lock().clear();
    }

    pub fn len(&self) -> usize {
        self.scripts.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.scripts.lock().is_empty()
    }
}

/// Metadata for one registered Redis Function inside a library.
#[derive(Debug, Clone)]
pub struct FunctionMeta {
    pub name: String,
    pub description: String,
    /// Redis function flags (e.g. `"no-writes"`).
    pub flags: Vec<String>,
}

/// One loaded Redis Functions library (Lua engine).
#[derive(Debug, Clone)]
pub struct FunctionLibrary {
    pub name: String,
    pub engine: String,
    pub code: String,
    pub functions: Vec<FunctionMeta>,
}

/// Parsed shebang from a library source (`#!lua name=mylib`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShebangInfo {
    pub engine: String,
    pub name: String,
}

/// Parse the first line of a Redis Functions library.
///
/// Expected form: `#!lua name=<libname>` (optional extra tokens ignored).
pub fn parse_function_shebang(code: &str) -> Result<ShebangInfo, String> {
    let first = code.lines().next().unwrap_or("").trim();
    if !first.starts_with("#!") {
        return Err(
            "ERR Library payload must begin with a #! shebang line (e.g. #!lua name=mylib)"
                .into(),
        );
    }
    let rest = first[2..].trim();
    let mut parts = rest.split_whitespace();
    let engine = parts
        .next()
        .ok_or_else(|| "ERR Missing engine name in shebang".to_string())?
        .to_string();
    if !engine.eq_ignore_ascii_case("lua") {
        return Err(format!(
            "ERR Engine '{}' is not supported (only LUA)",
            engine
        ));
    }
    let mut name: Option<String> = None;
    for tok in parts {
        if let Some(v) = tok.strip_prefix("name=") {
            if v.is_empty() {
                return Err("ERR Library name in shebang cannot be empty".into());
            }
            name = Some(v.to_string());
        }
    }
    let name = name.ok_or_else(|| {
        "ERR Library shebang must include name=<library-name>".to_string()
    })?;
    Ok(ShebangInfo {
        engine: "LUA".to_string(),
        name,
    })
}

/// Strip the leading `#!…` shebang line so Lua can execute the library body.
///
/// Redis keeps the shebang in stored source for DUMP/LIST WITHCODE, but Lua
/// does not treat `#!` as a comment.
pub fn strip_function_shebang(code: &str) -> &str {
    let trimmed = code.strip_prefix('\u{feff}').unwrap_or(code);
    if let Some(rest) = trimmed.strip_prefix("#!") {
        if let Some(nl) = rest.find('\n') {
            return rest[nl + 1..].trim_start_matches('\r');
        }
        // Shebang-only payload.
        return "";
    }
    code
}

/// Shared in-memory Redis Functions library store (FUNCTION LOAD / FCALL).
#[derive(Debug, Default)]
pub struct FunctionLibraryStore {
    inner: Mutex<FunctionStoreInner>,
}

#[derive(Debug, Default)]
struct FunctionStoreInner {
    /// library_name → library
    libraries: HashMap<String, FunctionLibrary>,
    /// function_name → library_name
    functions: HashMap<String, String>,
}

impl FunctionLibraryStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(FunctionStoreInner::default()),
        }
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().libraries.is_empty()
    }

    pub fn library_count(&self) -> usize {
        self.inner.lock().libraries.len()
    }

    /// Look up which library owns `function_name` and return a clone of that library.
    pub fn find_function(&self, function_name: &str) -> Option<FunctionLibrary> {
        let g = self.inner.lock();
        let lib_name = g.functions.get(function_name)?;
        g.libraries.get(lib_name).cloned()
    }

    /// Metadata for a single function (name, flags, description).
    pub fn function_meta(&self, function_name: &str) -> Option<FunctionMeta> {
        let g = self.inner.lock();
        let lib_name = g.functions.get(function_name)?;
        let lib = g.libraries.get(lib_name)?;
        lib.functions
            .iter()
            .find(|f| f.name == function_name)
            .cloned()
    }

    /// Insert or replace a library. On conflict without `replace`, returns an error string.
    pub fn load(
        &self,
        library: FunctionLibrary,
        replace: bool,
    ) -> Result<(), String> {
        let mut g = self.inner.lock();
        if g.libraries.contains_key(&library.name) {
            if !replace {
                return Err(format!(
                    "ERR Library '{}' already exists",
                    library.name
                ));
            }
            // Drop old library's function index entries first.
            if let Some(old) = g.libraries.remove(&library.name) {
                for f in &old.functions {
                    g.functions.remove(&f.name);
                }
            }
        }
        // Function name uniqueness across libraries.
        for f in &library.functions {
            if let Some(owner) = g.functions.get(&f.name) {
                if owner != &library.name {
                    return Err(format!(
                        "ERR Function {} already exists in library '{}'",
                        f.name, owner
                    ));
                }
            }
        }
        for f in &library.functions {
            g.functions
                .insert(f.name.clone(), library.name.clone());
        }
        g.libraries.insert(library.name.clone(), library);
        Ok(())
    }

    pub fn delete(&self, library_name: &str) -> Result<(), String> {
        let mut g = self.inner.lock();
        let lib = g
            .libraries
            .remove(library_name)
            .ok_or_else(|| format!("ERR Library not found '{}'", library_name))?;
        for f in &lib.functions {
            g.functions.remove(&f.name);
        }
        Ok(())
    }

    pub fn flush(&self) {
        let mut g = self.inner.lock();
        g.libraries.clear();
        g.functions.clear();
    }

    /// Snapshot libraries (sorted by name) for LIST / DUMP.
    pub fn list(&self) -> Vec<FunctionLibrary> {
        let g = self.inner.lock();
        let mut libs: Vec<_> = g.libraries.values().cloned().collect();
        libs.sort_by(|a, b| a.name.cmp(&b.name));
        libs
    }

    /// Filter by exact library name (Redis LIBRARYNAME is a pattern; we support exact / `*`).
    pub fn list_filtered(&self, libraryname: Option<&str>) -> Vec<FunctionLibrary> {
        let all = self.list();
        match libraryname {
            None => all,
            Some("*") => all,
            Some(pat) if pat.contains('*') || pat.contains('?') => {
                // Simple glob: * matches any substring, ? one char.
                all.into_iter()
                    .filter(|l| glob_match(pat, &l.name))
                    .collect()
            }
            Some(name) => all.into_iter().filter(|l| l.name == name).collect(),
        }
    }

    /// Serialize all libraries into a portable bulk payload (Kore `KORF1` format).
    pub fn dump(&self) -> Vec<u8> {
        let libs = self.list();
        let mut out = Vec::new();
        out.extend_from_slice(b"KORF1");
        out.extend_from_slice(&(libs.len() as u32).to_be_bytes());
        for lib in libs {
            write_len_str(&mut out, &lib.name);
            write_len_str(&mut out, &lib.code);
        }
        out
    }

    /// Parse dump payload into (name, code) pairs (no store mutation).
    pub fn parse_dump(payload: &[u8]) -> Result<Vec<(String, String)>, String> {
        parse_dump_payload(payload)
    }
}

fn write_len_str(out: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    out.extend_from_slice(&(b.len() as u32).to_be_bytes());
    out.extend_from_slice(b);
}

fn read_len_str(payload: &[u8], off: &mut usize) -> Result<String, String> {
    if *off + 4 > payload.len() {
        return Err("ERR Bad payload format".into());
    }
    let len = u32::from_be_bytes(payload[*off..*off + 4].try_into().unwrap()) as usize;
    *off += 4;
    if *off + len > payload.len() {
        return Err("ERR Bad payload format".into());
    }
    let s = String::from_utf8_lossy(&payload[*off..*off + len]).into_owned();
    *off += len;
    Ok(s)
}

fn parse_dump_payload(payload: &[u8]) -> Result<Vec<(String, String)>, String> {
    if payload.len() < 5 + 4 || &payload[..5] != b"KORF1" {
        return Err("ERR Bad payload format or version".into());
    }
    let mut off = 5;
    let count = u32::from_be_bytes(payload[off..off + 4].try_into().unwrap()) as usize;
    off += 4;
    let mut libs = Vec::with_capacity(count);
    for _ in 0..count {
        let name = read_len_str(payload, &mut off)?;
        let code = read_len_str(payload, &mut off)?;
        libs.push((name, code));
    }
    if off != payload.len() {
        return Err("ERR Bad payload format (trailing data)".into());
    }
    Ok(libs)
}

fn glob_match(pattern: &str, text: &str) -> bool {
    // Minimal glob: * and ?
    fn rec(p: &[u8], t: &[u8]) -> bool {
        match (p.first(), t.first()) {
            (None, None) => true,
            (Some(b'*'), _) => {
                // Match zero or more
                for i in 0..=t.len() {
                    if rec(&p[1..], &t[i..]) {
                        return true;
                    }
                }
                false
            }
            (Some(b'?'), Some(_)) => rec(&p[1..], &t[1..]),
            (Some(a), Some(b)) if a == b => rec(&p[1..], &t[1..]),
            _ => false,
        }
    }
    rec(pattern.as_bytes(), text.as_bytes())
}

/// Per-script execution flags (shared with Lua hooks / redis.call / SCRIPT KILL).
#[derive(Debug)]
pub struct ScriptRunFlags {
    id: u64,
    started: Instant,
    has_writes: AtomicBool,
    kill_requested: AtomicBool,
}

impl ScriptRunFlags {
    pub fn mark_write(&self) {
        self.has_writes.store(true, Ordering::SeqCst);
    }

    pub fn has_writes(&self) -> bool {
        self.has_writes.load(Ordering::SeqCst)
    }

    pub fn request_kill(&self) {
        self.kill_requested.store(true, Ordering::SeqCst);
    }

    pub fn kill_requested(&self) -> bool {
        self.kill_requested.load(Ordering::SeqCst)
    }

    pub fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }
}

/// Outcome of `SCRIPT KILL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptKillResult {
    Ok,
    NotBusy,
    Unkillable,
}

/// Server-wide script runtime: `lua-time-limit` + active script tracking for KILL.
#[derive(Debug)]
pub struct ScriptRuntime {
    lua_time_limit_ms: AtomicU64,
    next_id: AtomicU64,
    active: Mutex<Vec<Arc<ScriptRunFlags>>>,
}

impl Default for ScriptRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptRuntime {
    pub fn new() -> Self {
        Self {
            lua_time_limit_ms: AtomicU64::new(DEFAULT_LUA_TIME_LIMIT_MS),
            next_id: AtomicU64::new(1),
            active: Mutex::new(Vec::new()),
        }
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub fn time_limit_ms(&self) -> u64 {
        self.lua_time_limit_ms.load(Ordering::Relaxed)
    }

    pub fn set_time_limit_ms(&self, ms: u64) {
        self.lua_time_limit_ms.store(ms, Ordering::Relaxed);
    }

    /// Register a new script execution (paired with [`end`]).
    pub fn begin(&self) -> Arc<ScriptRunFlags> {
        let flags = Arc::new(ScriptRunFlags {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            started: Instant::now(),
            has_writes: AtomicBool::new(false),
            kill_requested: AtomicBool::new(false),
        });
        self.active.lock().push(Arc::clone(&flags));
        flags
    }

    /// Unregister a finished script execution.
    pub fn end(&self, flags: &ScriptRunFlags) {
        self.active.lock().retain(|f| f.id != flags.id);
    }

    pub fn active_count(&self) -> usize {
        self.active.lock().len()
    }

    /// `SCRIPT KILL` — request abort of active scripts that have not written.
    pub fn request_kill(&self) -> ScriptKillResult {
        let active = self.active.lock();
        if active.is_empty() {
            return ScriptKillResult::NotBusy;
        }
        if active.iter().any(|f| f.has_writes()) {
            return ScriptKillResult::Unkillable;
        }
        for f in active.iter() {
            f.request_kill();
        }
        ScriptKillResult::Ok
    }

    /// Check whether this run should abort (kill requested or hard time limit).
    ///
    /// Returns `Some(error_message)` when the script must stop.
    pub fn should_abort(&self, flags: &ScriptRunFlags) -> Option<&'static str> {
        if flags.kill_requested() {
            return Some("ERR script killed by user with SCRIPT KILL.");
        }
        let limit = self.time_limit_ms();
        // 0 = unlimited (Redis-compatible).
        if limit > 0 && flags.elapsed_ms() >= limit {
            return Some("ERR Lua script run time exceeded lua-time-limit");
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_stable() {
        // echo -n 'return 1' | sha1sum
        let sha = script_sha1("return 1");
        assert_eq!(sha.len(), 40);
        assert_eq!(sha, script_sha1("return 1"));
        assert_ne!(sha, script_sha1("return 2"));
    }

    #[test]
    fn load_get_exists_flush() {
        let c = ScriptCache::new();
        let body = "return redis.call('GET', KEYS[1])";
        let sha = c.load(body);
        assert_eq!(c.get(&sha).as_deref(), Some(body));
        assert!(c.get(&sha.to_uppercase()).is_some());
        assert_eq!(c.exists(&[sha.clone(), "deadbeef".into()]), vec![1, 0]);
        c.flush();
        assert!(c.is_empty());
        assert_eq!(c.exists(&[sha]), vec![0]);
    }

    #[test]
    fn shebang_parse() {
        let s = parse_function_shebang("#!lua name=mylib\nreturn 1").unwrap();
        assert_eq!(s.name, "mylib");
        assert_eq!(s.engine, "LUA");
        assert!(parse_function_shebang("return 1").is_err());
        assert!(parse_function_shebang("#!js name=x\n").is_err());
    }

    #[test]
    fn function_store_load_delete_dump_restore_parse() {
        let store = FunctionLibraryStore::new();
        let lib = FunctionLibrary {
            name: "mylib".into(),
            engine: "LUA".into(),
            code: "#!lua name=mylib\n".into(),
            functions: vec![FunctionMeta {
                name: "f1".into(),
                description: String::new(),
                flags: vec![],
            }],
        };
        store.load(lib, false).unwrap();
        assert!(store.find_function("f1").is_some());
        assert!(store.load(
            FunctionLibrary {
                name: "other".into(),
                engine: "LUA".into(),
                code: String::new(),
                functions: vec![FunctionMeta {
                    name: "f1".into(),
                    description: String::new(),
                    flags: vec![],
                }],
            },
            false
        )
        .is_err());

        let dump = store.dump();
        let pairs = FunctionLibraryStore::parse_dump(&dump).unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, "mylib");

        store.delete("mylib").unwrap();
        assert!(store.is_empty());
    }

    #[test]
    fn script_runtime_kill_and_time_limit() {
        let rt = ScriptRuntime::new();
        assert_eq!(rt.time_limit_ms(), DEFAULT_LUA_TIME_LIMIT_MS);
        rt.set_time_limit_ms(10);
        assert_eq!(rt.time_limit_ms(), 10);

        assert_eq!(rt.request_kill(), ScriptKillResult::NotBusy);

        let flags = rt.begin();
        assert_eq!(rt.active_count(), 1);
        assert_eq!(rt.request_kill(), ScriptKillResult::Ok);
        assert!(flags.kill_requested());
        assert!(rt.should_abort(&flags).unwrap().contains("SCRIPT KILL"));
        rt.end(&flags);
        assert_eq!(rt.active_count(), 0);

        let flags2 = rt.begin();
        flags2.mark_write();
        assert_eq!(rt.request_kill(), ScriptKillResult::Unkillable);
        rt.end(&flags2);
    }
}
