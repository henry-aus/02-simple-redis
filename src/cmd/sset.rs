use super::{extract_args, validate_command, CommandExecutor, SAdd, SIsMember};
use crate::{cmd::CommandError, RespArray, RespFrame};

impl CommandExecutor for SAdd {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        let count = self.values.len();
        backend.sset(self.key, self.values);
        RespFrame::Integer(count as i64)
    }
}

impl CommandExecutor for SIsMember {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        let sset = backend.sget(&self.key);
        let result = sset.and_then(|set| set.into_iter().find(|v| v == &self.value));

        match result {
            Some(_) => RespFrame::Integer(1),
            None => RespFrame::Integer(0),
        }
    }
}

impl TryFrom<RespArray> for SAdd {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let names = vec!["sadd"];

        if value.len() < 3 {
            return Err(CommandError::InvalidArgument(format!(
                "{} command must have at least {} argument",
                names.join(" "),
                2
            )));
        }

        validate_command(&value, &names, value.len() - names.len())?;

        let args = extract_args(value, 1)?;

        if let Some(RespFrame::BulkString(key)) = args.first() {
            Ok(SAdd {
                key: String::from_utf8(key.0.clone())?,
                values: args[1..].to_vec(),
            })
        } else {
            Err(CommandError::InvalidArgument("Invalid key".to_string()))
        }
    }
}

impl TryFrom<RespArray> for SIsMember {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["sismember"], 2)?;

        let mut args = extract_args(value, 1)?.into_iter();

        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(value)) => Ok(SIsMember {
                key: String::from_utf8(key.0)?,
                value,
            }),
            _ => Err(CommandError::InvalidArgument(
                "Invalid key, field or value".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::RespDecode;

    use super::*;
    use anyhow::{Ok, Result};
    use bytes::BytesMut;

    #[test]
    fn test_sadd_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*4\r\n$4\r\nsadd\r\n$3\r\nmap\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: SAdd = frame.try_into()?;
        assert_eq!(result.key, "map");
        assert_eq!(result.values, vec![b"hello".into(), b"world".into()]);

        Ok(())
    }

    #[test]
    fn test_sismember_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$9\r\nsismember\r\n$3\r\nmap\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: SIsMember = frame.try_into()?;
        assert_eq!(result.key, "map");
        assert_eq!(result.value, b"hello".into());

        Ok(())
    }

    #[test]
    fn test_sadd_sismember_command() -> Result<()> {
        let backend = crate::Backend::new();

        let sadd = SAdd {
            key: "map".to_string(),
            values: vec![b"hello".into(), b"world".into()],
        };

        let sadd_result = sadd.execute(&backend);
        assert_eq!(sadd_result, RespFrame::Integer(2));

        let sismember = SIsMember {
            key: "map".to_string(),
            value: b"hello".into(),
        };

        let sismember_result = sismember.execute(&backend);

        assert_eq!(sismember_result, RespFrame::Integer(1));

        Ok(())
    }
}
