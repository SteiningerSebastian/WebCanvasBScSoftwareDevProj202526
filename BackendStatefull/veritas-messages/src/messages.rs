use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub enum ParseError {
    ParameterNotFound(String),
    InvalidCommandType(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::ParameterNotFound(msg) => write!(f, "Error: Missing argument: {}", msg),
            ParseError::InvalidCommandType(msg) => write!(f, "Error: Wrong command type: {}", msg),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketCommand {
    pub command: String,
    pub parameters: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct WatchCommand {
    pub key: String,
}

impl WebsocketCommand {
    /// Converts a WebsocketCommand into a WatchCommand if possible.
    /// 
    /// Returns an error if the command type is incorrect or if the "key" parameter is missing.
    pub fn to_watch_command(&self) -> Result<WatchCommand, ParseError> {
        if self.command != "WatchCommand" {
            return Err(ParseError::InvalidCommandType(self.command.clone()));
        }
        
        // Look for the "key" parameter
        for (param_name, param_value) in &self.parameters {
            if param_name == "key" {
                return Ok(WatchCommand { key: param_value.clone() });
            }
        }
        Err(ParseError::ParameterNotFound("key".into()))
    }

    /// Creates a WebsocketCommand from a WatchCommand.
    /// 
    /// Returns a WebsocketCommand with the appropriate command type and parameters.
    pub fn from_watch_command(watch_cmd: &WatchCommand) -> WebsocketCommand {
        WebsocketCommand {
            command: "WatchCommand".into(),
            parameters: vec![("key".into(), watch_cmd.key.clone())],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketResponse {
    pub command: String,
    pub parameters: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct UpdateNotification{
    pub key: String,
    pub new_value: String,

    // Be aware that the old_value is NOT linerlizable or guranteed to be temporally ordered.
    // Only for the atomic operations get_and_add and compare_set old_value is guranteed to be the value before the operation.
    // This is because we do not want to load the value of the quorum before setting it, as this would introduce additional latency and complexity.
    // For the mentioned operations we must query the quorum anyway to ensure atomicity. (So we can get the old value at that point.)
    pub old_value: String,
}

impl WebsocketResponse {
    /// Converts a WebsocketResponse into an UpdateNotification if possible.
    /// 
    /// Returns an error if the command type is incorrect or if any required parameters are missing.
    pub fn to_update_notification(&self) -> Result<UpdateNotification, ParseError> {
        if self.command != "UpdateNotification" {
            return Err(ParseError::InvalidCommandType(self.command.clone()));
        }

        let mut key_opt: Option<String> = None;
        let mut new_value_opt: Option<String> = None;
        let mut old_value_opt: Option<String> = None;

        for (param_name, param_value) in &self.parameters {
            match param_name.as_str() {
                "key" => key_opt = Some(param_value.clone()),
                "new_value" => new_value_opt = Some(param_value.clone()),
                "old_value" => old_value_opt = Some(param_value.clone()),
                _ => {},
            }
        }

        if let (Some(key), Some(new_value), Some(old_value)) = (key_opt, new_value_opt, old_value_opt) {
            Ok(UpdateNotification { key, new_value, old_value })
        } else {
            Err(ParseError::ParameterNotFound("One or more parameters missing".into()))
        }
    }

    /// Creates a WebsocketResponse from an UpdateNotification.
    /// 
    /// Returns a WebsocketResponse with the appropriate command type and parameters.
    pub fn from_update_notification(update: &UpdateNotification) -> WebsocketResponse {
        WebsocketResponse {
            command: "UpdateNotification".into(),
            parameters: vec![
                ("key".into(), update.key.clone()),
                ("new_value".into(), update.new_value.clone()),
                ("old_value".into(), update.old_value.clone()),
            ],
        }
    }
}