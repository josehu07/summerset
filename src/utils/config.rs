//! Configuraiton parameters struct parsing helper.

/// Composes a configuration struct from its default values, then overwrites
/// given fields by parsing from given TOML string if it's not `None`. Returns
/// an `Ok(config)` on success, and `Err(SummersetError)` on parser failure.
///
/// Example:
/// ```no_run
/// let config = parsed_config!(config_str => MyConfig; batch_size, file_path)?;
/// ```
#[macro_export]
macro_rules! parsed_config {
    ($config_str:expr => $config_type:ty; $($field:ident),+) => {{
        let config_str: Option<&str> = $config_str;

        // closure helper for easier error returning
        let compose_config = || -> Result<$config_type, SummersetError> {
            let mut config: $config_type = Default::default();
            if let None = config_str {
                return Ok(config);
            }

            let mut table = config_str.unwrap().parse::<toml::Table>()?;

            // traverse through all given field names
            $({
                // if field name found in table (and removed)
                if let Some(v) = table.remove(stringify!($field)) {
                    config.$field = v.try_into()?;
                }
            })+

            // if table is not empty at this time, some parsed keys are not
            // expected hence invalid
            if table.len() > 0 {
                return Err(SummersetError(format!(
                    "invalid field name '{}' in config",
                    table.keys().next().unwrap(),
                )));
            }

            Ok(config)
        };

        compose_config()
    }};
}

#[cfg(test)]
mod config_tests {
    use crate::utils::SummersetError;

    #[derive(Debug, PartialEq)]
    struct TestConfig {
        abc: u16,
        hij: String,
        lmn: f64,
    }

    impl Default for TestConfig {
        fn default() -> Self {
            TestConfig {
                abc: 7,
                hij: "Jose".into(),
                lmn: 6.18,
            }
        }
    }

    #[test]
    fn parse_from_none() -> Result<(), SummersetError> {
        let config = parsed_config!(None => TestConfig; abc, hij, lmn)?;
        let ref_config: TestConfig = Default::default();
        assert_eq!(config, ref_config);
        Ok(())
    }

    #[test]
    fn parse_from_partial() -> Result<(), SummersetError> {
        let config_str = Some("hij = 'Nice'");
        let config = parsed_config!(config_str => TestConfig; hij, lmn)?;
        let ref_config = TestConfig {
            abc: 7,
            hij: "Nice".into(),
            lmn: 6.18,
        };
        assert_eq!(config, ref_config);
        Ok(())
    }

    #[test]
    fn parse_invalid_field() {
        let config_str = Some("xyz = 999");
        assert!(parsed_config!(config_str => TestConfig; abc).is_err());
    }
}
