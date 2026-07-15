//! ACL command handlers: SETUSER, GETUSER, LIST, WHOAMI, CAT, DELUSER, LOAD, SAVE.

use super::CommandHandler;
use crate::acl::{category_commands, category_names, AclUser};
use crate::error::Result;
use crate::protocol::RespValue;
use bytes::Bytes;

fn bulk(s: impl Into<Bytes>) -> RespValue {
    RespValue::BulkString(Some(s.into()))
}

impl CommandHandler {
    pub(super) fn handle_acl(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'acl' command",
            ));
        }
        let sub = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR invalid ACL subcommand")),
        };

        match sub.as_str() {
            "SETUSER" => self.acl_setuser(&args[1..]),
            "GETUSER" => self.acl_getuser(&args[1..]),
            "LIST" => self.acl_list(&args[1..]),
            "USERS" => self.acl_users(&args[1..]),
            "WHOAMI" => self.acl_whoami(&args[1..]),
            "CAT" => self.acl_cat(&args[1..]),
            "DELUSER" => self.acl_deluser(&args[1..]),
            "LOAD" => self.acl_load(&args[1..]),
            "SAVE" => self.acl_save(&args[1..]),
            "HELP" => Ok(acl_help()),
            _ => Ok(RespValue::error(format!(
                "ERR Unknown subcommand or wrong number of arguments for '{}'. Try ACL HELP.",
                sub
            ))),
        }
    }

    fn acl_setuser(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'acl|setuser' command",
            ));
        }
        let username = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid username")),
        };
        let mut rules: Vec<String> = Vec::new();
        for a in &args[1..] {
            match a.as_bulk_string() {
                Some(b) => rules.push(String::from_utf8_lossy(b).into_owned()),
                None => return Ok(RespValue::error("ERR invalid ACL rule")),
            }
        }
        let rule_refs: Vec<&str> = rules.iter().map(|s| s.as_str()).collect();
        match self.acl.setuser(&username, &rule_refs) {
            Ok(()) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    fn acl_getuser(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'acl|getuser' command",
            ));
        }
        let username = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid username")),
        };
        match self.acl.get_user(&username) {
            Some(user) => Ok(getuser_reply(&user)),
            None => Ok(RespValue::null()),
        }
    }

    fn acl_list(&self, args: &[RespValue]) -> Result<RespValue> {
        if !args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'acl|list' command",
            ));
        }
        let entries = self.acl.list_users();
        Ok(RespValue::Array(entries.into_iter().map(bulk).collect()))
    }

    fn acl_users(&self, args: &[RespValue]) -> Result<RespValue> {
        if !args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'acl|users' command",
            ));
        }
        Ok(RespValue::Array(
            self.acl.usernames().into_iter().map(bulk).collect(),
        ))
    }

    fn acl_whoami(&self, args: &[RespValue]) -> Result<RespValue> {
        if !args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'acl|whoami' command",
            ));
        }
        match &self.username {
            Some(u) if self.authenticated => Ok(bulk(u.clone())),
            _ => Ok(RespValue::error("ERR no username bound to connection")),
        }
    }

    fn acl_cat(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::Array(
                category_names().iter().map(|c| bulk(*c)).collect(),
            ));
        }
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'acl|cat' command",
            ));
        }
        let cat = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid category name")),
        };
        match category_commands(&cat) {
            Ok(cmds) => Ok(RespValue::Array(cmds.into_iter().map(bulk).collect())),
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    fn acl_deluser(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'acl|deluser' command",
            ));
        }
        let mut names: Vec<String> = Vec::new();
        for a in args {
            match a.as_bulk_string() {
                Some(b) => names.push(String::from_utf8_lossy(b).into_owned()),
                None => return Ok(RespValue::error("ERR invalid username")),
            }
        }
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        match self.acl.deluser(&refs) {
            Ok(n) => Ok(RespValue::Integer(n as i64)),
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    fn acl_load(&self, args: &[RespValue]) -> Result<RespValue> {
        if !args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'acl|load' command",
            ));
        }
        match self.acl.load() {
            Ok(()) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    fn acl_save(&self, args: &[RespValue]) -> Result<RespValue> {
        if !args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'acl|save' command",
            ));
        }
        match self.acl.save() {
            Ok(()) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(e)),
        }
    }
}

fn getuser_reply(user: &AclUser) -> RespValue {
    let mut flags = Vec::new();
    flags.push(if user.enabled {
        bulk("on")
    } else {
        bulk("off")
    });
    if user.nopass {
        flags.push(bulk("nopass"));
    }
    if user.all_keys {
        flags.push(bulk("allkeys"));
    }
    if user.all_commands {
        flags.push(bulk("allcommands"));
    }
    if user.all_channels {
        flags.push(bulk("allchannels"));
    }

    let passwords: Vec<RespValue> = if user.nopass {
        Vec::new()
    } else {
        // Redis returns SHA256 hex hashes; expose opaque markers for shape compatibility.
        user.passwords
            .iter()
            .enumerate()
            .map(|(i, _)| bulk(format!("pass{}", i)))
            .collect()
    };

    let commands = if user.command_desc.is_empty() {
        if user.all_commands {
            "+@all".to_string()
        } else {
            "-@all".to_string()
        }
    } else {
        user.command_desc.clone()
    };

    let keys: Vec<RespValue> = if user.all_keys {
        vec![bulk("~*")]
    } else {
        user.key_patterns
            .iter()
            .map(|p| bulk(format!("~{}", p)))
            .collect()
    };

    let channels: Vec<RespValue> = if user.all_channels {
        vec![bulk("&*")]
    } else {
        user.channel_patterns
            .iter()
            .map(|p| bulk(format!("&{}", p)))
            .collect()
    };

    RespValue::Array(vec![
        bulk("flags"),
        RespValue::Array(flags),
        bulk("passwords"),
        RespValue::Array(passwords),
        bulk("commands"),
        bulk(commands),
        bulk("keys"),
        RespValue::Array(keys),
        bulk("channels"),
        RespValue::Array(channels),
        bulk("selectors"),
        RespValue::Array(vec![]),
    ])
}

fn acl_help() -> RespValue {
    RespValue::Array(
        [
            "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "CAT [<category>]",
            "    List categories or commands in a category.",
            "DELUSER <username> [<username> ...]",
            "    Delete one or more users. The default user cannot be removed.",
            "GETUSER <username>",
            "    Return the rules for a user.",
            "LIST",
            "    Show user details as ACL configuration rules.",
            "LOAD",
            "    Reload users from the configured ACL file.",
            "SAVE",
            "    Save the current ACL rules to the configured ACL file.",
            "SETUSER <username> [rules...]",
            "    Create or modify a user.",
            "USERS",
            "    List all usernames.",
            "WHOAMI",
            "    Return the current connection username.",
            "HELP",
            "    Print this help.",
        ]
        .iter()
        .map(|s| bulk(*s))
        .collect(),
    )
}
