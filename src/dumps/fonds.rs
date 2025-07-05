use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum Fond {
    JORF,
    CNIL,
    JADE,
    LEGI,
    INCA,
    CASS,
    CAPP,
}

// implement ValueEnum for Fond
// so that it can be used in clap
// (and the help message will show the list of possible tarballs)
use clap::ValueEnum;
impl ValueEnum for Fond {
    fn value_variants<'a>() -> &'a [Self] {
        &FONDS
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(clap::builder::PossibleValue::new(self.as_str()))
    }
}

impl TryFrom<String> for Fond {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "JORF" => Ok(Fond::JORF),
            "CNIL" => Ok(Fond::CNIL),
            "JADE" => Ok(Fond::JADE),
            "LEGI" => Ok(Fond::LEGI),
            "INCA" => Ok(Fond::INCA),
            "CASS" => Ok(Fond::CASS),
            "CAPP" => Ok(Fond::CAPP),
            _ => Err(anyhow::anyhow!("Invalid fond")),
        }
    }
}

/// Implement as_str for Fond
impl Fond {
    pub fn as_str(&self) -> &'static str {
        match self {
            Fond::JORF => "JORF",
            Fond::CNIL => "CNIL",
            Fond::JADE => "JADE",
            Fond::LEGI => "LEGI",
            Fond::INCA => "INCA",
            Fond::CASS => "CASS",
            Fond::CAPP => "CAPP",
        }
    }
}

impl std::fmt::Display for Fond {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// List of tarballs in the dila server
pub const FONDS: &[Fond] = &[
    Fond::JORF,
    Fond::CNIL,
    Fond::JADE,
    Fond::LEGI,
    Fond::INCA,
    Fond::CASS,
    Fond::CAPP,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fond() {
        let fond = Fond::JORF;
        assert_eq!(fond.as_str(), "JORF");
        assert_eq!(fond.to_string(), "JORF");
        assert_eq!(fond, Fond::JORF);
        assert_eq!(fond, Fond::try_from("JORF".to_string()).unwrap());
    }

    #[test]
    fn test_fond_as_str() {
        for fond in FONDS {
            let str = fond.as_str();
            let fond2 = Fond::try_from(str.to_string());
            assert!(fond2.is_ok());
            assert_eq!(fond, &fond2.unwrap());
        }
    }
}
