/// This module contains helper functions to extract metadata
/// and text from XML files stored in the DILA achive dumps.
/// It also contains normalization functions to clean up
/// these extracted metadata.
///
/// Note that for now, no normalization is provided. Also,
/// only a few tags are actually extracted, and we assume
/// that the XML files have a specific format that is not
/// yet documented.
use quick_xml::events::Event;
use quick_xml::reader::Reader;

use serde::{Deserialize, Serialize};

use std::path::PathBuf;

pub mod law_extraction {
    use once_cell::sync::OnceCell;
    use regex::Regex;
    use std::collections::HashMap;

    #[derive(Eq, Hash, PartialEq)]
    pub struct LawCode {
        prefix: String,
        number: String,
    }

    pub fn law_regex() -> &'static Regex {
        static INSTANCE: OnceCell<Regex> = OnceCell::new();
        INSTANCE.get_or_init(|| {
            Regex::new(r"([A-Z])\.\s+([0-9-]+)").expect("Unable to construct law searching regex")
        })
    }

    pub fn law_uses(s: &str, count: &mut HashMap<LawCode, usize>) {
        let re = law_regex();
        for (_, [prefix, number]) in re.captures_iter(s).map(|c| c.extract()) {
            let law = LawCode {
                prefix: prefix.to_string(),
                number: number.to_string(),
            };
            count.entry(law).and_modify(|c| *c += 1).or_insert(1);
        }
    }
}

/// This function takes a file that contains XML data
/// and a mutable HashMap that will be filled with the
/// count of each tag in the XML file.
pub fn count_tags_in_file(
    file: &PathBuf,
    tag_count: &mut std::collections::HashMap<String, usize>,
) {
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(file).unwrap();
    let mut buffer = String::new();
    file.read_to_string(&mut buffer).unwrap();

    let mut reader = Reader::from_str(&buffer);
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let e = tag_count.entry(tag).or_insert(0);
                *e += 1;
            }
            Ok(Event::End(e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let e = tag_count.entry(tag).or_insert(0);
                *e += 1;
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
    }
}

/// PreDilaText is a struct that contains the metadata and text
/// of a decision from the DILA database. The inner metadata is
/// not parsed and normalized yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreDilaText {
    /// The unique identifier of the decision in the DILA database
    pub id: String,
    /// The unique identifier of the decision in the DILA database
    /// before the 2015 reform
    pub oldid: String,
    /// Dataset origin of the decision (e.g. "CETAT")
    pub origin: String,
    /// The relative path to the XML file in the DILA archive
    pub url: String,
    /// The type of decision (e.g. "Texte", "Arrêt", "Ordonnance")
    pub nature: String,
    /// A potential title for the decision (usually redundant with the metadata)
    pub title: Option<String>,
    /// The date of the decision in the format YYYY-MM-DD
    pub decision_date: Option<String>,
    /// The jurisdiction of the decision (e.g. "Cour de cassation", "Conseil d'Etat", etc.)
    pub jurisdiction: Option<String>,
    /// A number that identifies the decision in the jurisdiction
    pub juri_code: Option<String>,
    /// "Demandeur" is the party that made the request
    pub requester: Option<String>,
    /// President is the judge that made the decision
    pub president: Option<String>,
    /// "Avocat" is the lawyer that made the request
    pub lawyers: Option<String>,
    /// "Rapporteur" is the judge that made the report
    pub rapporteur: Option<String>,
    /// "Commissaire gouvernement" is the government commissioner
    pub government_commissioner: Option<String>,
    /// "ECLI" is an identifier for the decision in the European Court of Justice
    pub ecli_code: Option<String>,
    /// The full text of the decision (contains <br/> tags in addition to linebreaks)
    pub text: String,
}

impl Default for PreDilaText {
    fn default() -> Self {
        PreDilaText {
            id: String::new(),
            oldid: String::new(),
            origin: String::new(),
            url: String::new(),
            nature: String::new(),
            title: None,
            decision_date: None,
            jurisdiction: None,
            juri_code: None,
            requester: None,
            president: None,
            lawyers: None,
            rapporteur: None,
            government_commissioner: None,
            ecli_code: None,
            text: String::new(),
        }
    }
}

/// This enum is used to keep track of the current state
/// of the reader while parsing the XML file.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReadingState {
    ID,
    OldID,
    Origin,
    URL,
    Nature,
    Title,
    DecisionDate,
    Jurisdiction,
    JuriCode,
    Requester,
    President,
    Lawyers,
    Rapporteur,
    GovernmentCommissioner,
    ECLICode,
    Text,
}

/// This function updates the PreDilaText struct with the
/// current reading state and the text that was read.
fn update_pre_dila(pre_dila: &mut PreDilaText, reading_state: &Option<ReadingState>, text: &str) {
    if let Some(state) = reading_state {
        match state {
            ReadingState::ID => pre_dila.id = text.to_string(),
            ReadingState::OldID => pre_dila.oldid = text.to_string(),
            ReadingState::Origin => pre_dila.origin = text.to_string(),
            ReadingState::URL => pre_dila.url = text.to_string(),
            ReadingState::Nature => pre_dila.nature = text.to_string(),
            ReadingState::Title => pre_dila.title = Some(text.to_string()),
            ReadingState::DecisionDate => pre_dila.decision_date = Some(text.to_string()),
            ReadingState::Jurisdiction => pre_dila.jurisdiction = Some(text.to_string()),
            ReadingState::JuriCode => pre_dila.juri_code = Some(text.to_string()),
            ReadingState::Requester => pre_dila.requester = Some(text.to_string()),
            ReadingState::President => pre_dila.president = Some(text.to_string()),
            ReadingState::Lawyers => pre_dila.lawyers = Some(text.to_string()),
            ReadingState::Rapporteur => pre_dila.rapporteur = Some(text.to_string()),
            ReadingState::GovernmentCommissioner => {
                pre_dila.government_commissioner = Some(text.to_string())
            }
            ReadingState::ECLICode => pre_dila.ecli_code = Some(text.to_string()),
            ReadingState::Text => pre_dila.text.push_str(text),
        }
    }
}

fn event_to_reading_state(e: &[u8]) -> Option<ReadingState> {
    match e {
        b"ID" => Some(ReadingState::ID),
        b"ANCIEN_ID" => Some(ReadingState::OldID),
        b"ORIGINE" => Some(ReadingState::Origin),
        b"URL" => Some(ReadingState::URL),
        b"NATURE" => Some(ReadingState::Nature),
        b"TITRE" => Some(ReadingState::Title),
        b"DATE_DEC" => Some(ReadingState::DecisionDate),
        b"JURIDICTION" => Some(ReadingState::Jurisdiction),
        b"NUMERO" => Some(ReadingState::JuriCode),
        b"DEMANDEUR" => Some(ReadingState::Requester),
        b"PRESIDENT" => Some(ReadingState::President),
        b"AVOCATS" => Some(ReadingState::Lawyers),
        b"RAPPORTEUR" => Some(ReadingState::Rapporteur),
        b"COMMISSAIRE_GVT" => Some(ReadingState::GovernmentCommissioner),
        b"ECLI" => Some(ReadingState::ECLICode),
        b"CONTENU" => Some(ReadingState::Text),
        _ => None,
    }
}

/// This function reads an XML file and returns a PreDilaText struct
/// with the metadata and text of the decision.
fn reader_to_pre_dila(r: &mut Reader<&[u8]>) -> PreDilaText {
    let mut pre_dila = PreDilaText::default();

    let mut reading_state = None;

    loop {
        let event = r.read_event();
        match &event {
            Err(e) => {
                eprintln!("Error at position {}: {:?}", r.buffer_position(), e);
                break;
            }
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => {
                let e = e.name();
                if let Some(s) = event_to_reading_state(e.as_ref()) {
                    reading_state = Some(s);
                }
            }
            Ok(Event::End(e)) => {
                let e = e.name();
                let s = event_to_reading_state(e.as_ref());
                if s == reading_state {
                    reading_state = None;
                }
            }
            Ok(Event::Text(t)) => {
                let txt = t.unescape().unwrap();
                update_pre_dila(&mut pre_dila, &reading_state, &txt);
            }
            _ => {}
        }
    }

    pre_dila
}

/// This function reads from a io::Read and returns a PreDilaText struct
/// with the metadata and text of the decision.
pub fn parse_file(file: &PathBuf, buf: &mut String) -> PreDilaText {
    use std::fs::File;
    use std::io::Read;
    use std::path::PathBuf;

    let file = PathBuf::from(file);
    if !file.exists() {
        panic!("File does not exist: {}", file.display());
    }
    if !file.is_file() {
        panic!("Path is not a file: {}", file.display());
    }
    if file.extension().unwrap_or_default() != "xml" {
        panic!("File is not an XML file: {}", file.display());
    }
    let mut file = File::open(file).unwrap();
    file.read_to_string(buf).unwrap();
    let mut reader = Reader::from_str(&buf);
    reader_to_pre_dila(&mut reader)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE_XML: &str = r#"
<?xml version="1.0" encoding="UTF-8"?>
<TEXTE_JURI_ADMIN>
<META>
<META_COMMUN>
<ID>CETATEXT000049314894</ID>
<ANCIEN_ID>JG_L_2024_03_000000490536</ANCIEN_ID>
<ORIGINE>CETAT</ORIGINE>
<URL>texte/juri/admin/CETA/TEXT/00/00/49/31/48/CETATEXT000049314894.xml</URL>
<NATURE>Texte</NATURE>
</META_COMMUN>
<META_SPEC>
<META_JURI>
<TITRE>Conseil d'État, 2ème - 7ème chambres réunies, 21/03/2024, 490536</TITRE>
<DATE_DEC>2024-03-21</DATE_DEC>
<JURIDICTION>Conseil d'État</JURIDICTION>
<NUMERO>490536</NUMERO>
<SOLUTION/>
</META_JURI>
<META_JURI_ADMIN>

<FORMATION>2ème - 7ème chambres réunies</FORMATION>
<TYPE_REC>Autres</TYPE_REC>
<PUBLI_RECUEIL>B</PUBLI_RECUEIL>
<DEMANDEUR/>
<DEFENDEUR/>
<PRESIDENT/>
<AVOCATS>SCP BAUER-VIOLAS - FESCHOTTE-DESBOIS - SEBAGH ; SCP MARLANGE, DE LA BURGADE ; SCP SPINOSI</AVOCATS>
<RAPPORTEUR>M. Alexandre Trémolière</RAPPORTEUR>
<COMMISSAIRE_GVT>M. Clément Malverti</COMMISSAIRE_GVT>
<ECLI>ECLI:FR:CECHR:2024:490536.20240321</ECLI>
</META_JURI_ADMIN>
</META_SPEC>
</META>
<TEXTE>
<BLOC_TEXTUEL>
<CONTENU>
              2.	En vertu de l'article R. 421-1 du code de l'urbanisme, les constructions nouvelles doivent être précédées de la délivrance d'un permis de construire à l'exception des constructions mentionnées aux articles R. 421-2 à R. 421-8, qui sont dispensées de toute formalité au titre du code de l'urbanisme, et des constructions mentionnées aux articles R. 421-9 à R. 421-12, qui doivent faire l'objet d'une déclaration préalable. Selon le a) de l'article R. 421-2 du même code, les constructions nouvelles dont la hauteur au-dessus du sol est inférieure à douze mètres et qui ont pour effet de créer une surface de plancher et une emprise au sol inférieures ou égales à cinq mètres carrés sont dispensées, en dehors des secteurs sauvegardés et des sites classés, de toute formalité au titre du code de l'urbanisme. Aux termes de l'article R. 421-9 du même code, dans sa rédaction issue du décret du 10 décembre 2018 relatif à l'extension du régime de la déclaration préalable aux projets d'installation d'antennes-relais de radiotéléphonie mobile et à leurs locaux ou installations techniques au titre du code de l'urbanisme : " En dehors du périmètre des sites patrimoniaux remarquables, des abords des monuments historiques et des sites classés ou en instance de classement, les constructions nouvelles suivantes doivent être précédées d'une déclaration préalable, à l'exception des cas mentionnés à la sous-section 2 ci-dessus : / (...) c) Les constructions répondant aux critères cumulatifs suivants : / - une hauteur au-dessus du sol supérieure à douze mètres ; / - une emprise au sol inférieure ou égale à cinq mètres carrés ; / - une surface de plancher inférieure ou égale à cinq mètres carrés. / Toutefois, ces dispositions ne sont applicables ni aux éoliennes, ni aux ouvrages de production d'électricité à partir de l'énergie solaire installés au sol, ni aux antennes-relais de radiotéléphonie mobile ; (...) / j) Les antennes-relais de radiotéléphonie mobile et leurs systèmes d'accroche, quelle que soit leur hauteur, et les locaux ou installations techniques nécessaires à leur fonctionnement dès lors que ces locaux ou installations techniques ont une surface de plancher et une emprise au sol supérieures à 5 m² et inférieures ou égales à 20 m² ".<br/>
<br/>
</CONTENU>
</BLOC_TEXTUEL>
<SOMMAIRE>
<SCT ID="8A" TYPE="PRINCIPAL">51-02-01 POSTES ET COMMUNICATIONS ÉLECTRONIQUES. - COMMUNICATIONS ÉLECTRONIQUES. - TÉLÉPHONE. - CONSTRUCTION NOUVELLE D’ANTENNES-RELAIS DE RADIOTÉLÉPHONIE MOBILE EN DEHORS DES SECTEURS PROTÉGÉS – 1) A) PROJETS SOUMIS À DÉCLARATION PRÉALABLE – I) POUR TOUTES LES ANTENNES – SURFACE DE PLANCHER ET EMPRISE AU SOL ENTRE 5 ET 20 M² – II) POUR LES ANTENNES DE PLUS DE 12 M – SURFACE DE PLANCHER ET EMPRISE AU SOL INFÉRIEURES À 5 M² – B) PROJETS DISPENSÉS DE TOUTE FORMALITÉ – ANTENNES DE MOINS DE 12 M ENTRAÎNANT LA CRÉATION D’UNE SURFACE DE PLANCHER ET D’UNE EMPRISE AU SOL INFÉRIEURES OU ÉGALES À 5 M² – 2) APPRÉCIATION DES SEUILS DE SURFACE DE PLANCHER ET D’EMPRISE AU SOL – INCLUSION – SURFACE ET EMPRISE DES LOCAUX ET INSTALLATIONS TECHNIQUES – EXCLUSION – EMPRISE DES PYLÔNES [RJ1].
</SCT>
<SCT ID="8B" TYPE="PRINCIPAL">68-03-01-02 URBANISME ET AMÉNAGEMENT DU TERRITOIRE. - PERMIS DE CONSTRUIRE. - TRAVAUX SOUMIS AU PERMIS. - NE PRÉSENTENT PAS CE CARACTÈRE. - CONSTRUCTION NOUVELLE D’ANTENNES-RELAIS DE RADIOTÉLÉPHONIE MOBILE EN DEHORS DES SECTEURS PROTÉGÉS – 1) A) PROJETS SOUMIS À DÉCLARATION PRÉALABLE – I) POUR TOUTES LES ANTENNES – SURFACE DE PLANCHER ET EMPRISE AU SOL ENTRE 5 ET 20 M² – II) POUR LES ANTENNES DE PLUS DE 12 M – SURFACE DE PLANCHER ET EMPRISE AU SOL INFÉRIEURES À 5 M² – B) PROJETS DISPENSÉS DE TOUTE FORMALITÉ – ANTENNES DE MOINS DE 12 M ENTRAÎNANT LA CRÉATION D’UNE SURFACE DE PLANCHER ET D’UNE EMPRISE AU SOL INFÉRIEURES OU ÉGALES À 5 M² – 2) APPRÉCIATION DES SEUILS DE SURFACE DE PLANCHER ET D’EMPRISE AU SOL – INCLUSION – SURFACE ET EMPRISE DES LOCAUX ET INSTALLATIONS TECHNIQUES – EXCLUSION – EMPRISE DES PYLÔNES [RJ1].
</SCT>
<SCT ID="8C" TYPE="PRINCIPAL">68-04-045 URBANISME ET AMÉNAGEMENT DU TERRITOIRE. - AUTORISATIONS D`UTILISATION DES SOLS DIVERSES. - RÉGIMES DE DÉCLARATION PRÉALABLE. - CONSTRUCTION NOUVELLE D’ANTENNES-RELAIS DE RADIOTÉLÉPHONIE MOBILE EN DEHORS DES SECTEURS PROTÉGÉS – 1) A) PROJETS SOUMIS À DÉCLARATION PRÉALABLE – I) POUR TOUTES LES ANTENNES – SURFACE DE PLANCHER ET EMPRISE AU SOL ENTRE 5 ET 20 M² – II) POUR LES ANTENNES DE PLUS DE 12 M – SURFACE DE PLANCHER ET EMPRISE AU SOL INFÉRIEURES À 5 M² – B) PROJETS DISPENSÉS DE TOUTE FORMALITÉ – ANTENNES DE MOINS DE 12 M ENTRAÎNANT LA CRÉATION D’UNE SURFACE DE PLANCHER ET D’UNE EMPRISE AU SOL INFÉRIEURES OU ÉGALES À 5 M² – 2) APPRÉCIATION DES SEUILS DE SURFACE DE PLANCHER ET D’EMPRISE AU SOL – INCLUSION – SURFACE ET EMPRISE DES LOCAUX ET INSTALLATIONS TECHNIQUES – EXCLUSION – EMPRISE DES PYLÔNES [RJ1].
</SCT>
<ANA ID="9A"> 51-02-01 1) a) Les c et j de l’article R. 421-9 du code de l’urbanisme, dans leur rédaction issue du décret n° 2018-1123 du 10 décembre 2018, doivent être lus, au regard de l’objet des modifications opérées par ce décret, comme soumettant à la procédure de déclaration préalable la construction d’antennes-relais de radiotéléphonie mobile, de leurs systèmes d'accroche, et des locaux ou installations techniques nécessaires à leur fonctionnement lorsque i) soit, quelle que soit la hauteur de l’antenne, la surface de plancher et l'emprise au sol créées sont supérieures à 5 mètres carrés et inférieure ou égale à 20 mètres carrés, ii) soit, s’agissant des antennes d’une hauteur supérieure à douze mètres, la surface de plancher et l'emprise au sol créées sont inférieures ou égales à 5 mètres carrés. ...b) Les projets comportant des antennes d’une hauteur inférieure ou égale à 12 mètres et entraînant la création d’une surface de plancher et d’une emprise au sol inférieures ou égales à 5 mètres carrés restent dispensés de toute formalité en application des dispositions de l’article R. 421-2....2) Pour l’appréciation des seuils applicables à ces projets de constructions, s’agissant tant de ceux fixés au j de l’article R. 421-9 du code de l’urbanisme, que de ceux mentionnés au c de cet article et au a de l’article R. 421-2, seules la surface de plancher et l’emprise au sol des locaux et installations techniques doivent être prises en compte, et non l’emprise au sol des pylônes.</ANA>
<ANA ID="9B"> 68-03-01-02 1) a) Les c et j de l’article R. 421-9 du code de l’urbanisme, dans leur rédaction issue du décret n° 2018-1123 du 10 décembre 2018, doivent être lus, au regard de l’objet des modifications opérées par ce décret, comme soumettant à la procédure de déclaration préalable la construction d’antennes-relais de radiotéléphonie mobile, de leurs systèmes d'accroche, et des locaux ou installations techniques nécessaires à leur fonctionnement lorsque i) soit, quelle que soit la hauteur de l’antenne, la surface de plancher et l'emprise au sol créées sont supérieures à 5 mètres carrés et inférieure ou égale à 20 mètres carrés, ii) soit, s’agissant des antennes d’une hauteur supérieure à douze mètres, la surface de plancher et l'emprise au sol créées sont inférieures ou égales à 5 mètres carrés. ...b) Les projets comportant des antennes d’une hauteur inférieure ou égale à 12 mètres et entraînant la création d’une surface de plancher et d’une emprise au sol inférieures ou égales à 5 mètres carrés restent dispensés de toute formalité en application des dispositions de l’article R. 421-2....2) Pour l’appréciation des seuils applicables à ces projets de constructions, s’agissant tant de ceux fixés au j de l’article R. 421-9 du code de l’urbanisme, que de ceux mentionnés au c de cet article et au a de l’article R. 421-2, seules la surface de plancher et l’emprise au sol des locaux et installations techniques doivent être prises en compte, et non l’emprise au sol des pylônes.</ANA>
<ANA ID="9C"> 68-04-045 1) a) Les c et j de l’article R. 421-9 du code de l’urbanisme, dans leur rédaction issue du décret n° 2018-1123 du 10 décembre 2018, doivent être lus, au regard de l’objet des modifications opérées par ce décret, comme soumettant à la procédure de déclaration préalable la construction d’antennes-relais de radiotéléphonie mobile, de leurs systèmes d'accroche, et des locaux ou installations techniques nécessaires à leur fonctionnement lorsque i) soit, quelle que soit la hauteur de l’antenne, la surface de plancher et l'emprise au sol créées sont supérieures à 5 mètres carrés et inférieure ou égale à 20 mètres carrés, ii) soit, s’agissant des antennes d’une hauteur supérieure à douze mètres, la surface de plancher et l'emprise au sol créées sont inférieures ou égales à 5 mètres carrés. ...b) Les projets comportant des antennes d’une hauteur inférieure ou égale à 12 mètres et entraînant la création d’une surface de plancher et d’une emprise au sol inférieures ou égales à 5 mètres carrés restent dispensés de toute formalité en application des dispositions de l’article R. 421-2....2) Pour l’appréciation des seuils applicables à ces projets de constructions, s’agissant tant de ceux fixés au j de l’article R. 421-9 du code de l’urbanisme, que de ceux mentionnés au c de cet article et au a de l’article R. 421-2, seules la surface de plancher et l’emprise au sol des locaux et installations techniques doivent être prises en compte, et non l’emprise au sol des pylônes.</ANA>
</SOMMAIRE>

<CITATION_JP>
<CONTENU>[RJ1] Comp., avant l’intervention du décret n° 2018-1123 du 10 décembre 2018, CE, 20 juin 2012, M. Richard et autres, n° 344646, T. pp. 889-1023.</CONTENU>
</CITATION_JP>
</TEXTE>
<LIENS/>
</TEXTE_JURI_ADMIN>"#;

    #[test]
    fn test_pre_dila_metadata_parser() {
        let mut reader = Reader::from_reader(EXAMPLE_XML.as_bytes());
        let pre_dila = reader_to_pre_dila(&mut reader);

        assert_eq!(pre_dila.id, "CETATEXT000049314894");
        assert_eq!(pre_dila.oldid, "JG_L_2024_03_000000490536");
        assert_eq!(pre_dila.origin, "CETAT");
        assert_eq!(
            pre_dila.url,
            "texte/juri/admin/CETA/TEXT/00/00/49/31/48/CETATEXT000049314894.xml"
        );
        assert_eq!(pre_dila.nature, "Texte");
        assert_eq!(
            pre_dila.title,
            Some("Conseil d'État, 2ème - 7ème chambres réunies, 21/03/2024, 490536".to_string())
        );
        assert_eq!(pre_dila.decision_date, Some("2024-03-21".to_string()));
        assert_eq!(pre_dila.jurisdiction, Some("Conseil d'État".to_string()));
        assert_eq!(pre_dila.juri_code, Some("490536".to_string()));
        assert_eq!(pre_dila.requester, None);
        assert_eq!(pre_dila.president, None);
        assert_eq!(pre_dila.lawyers, Some("SCP BAUER-VIOLAS - FESCHOTTE-DESBOIS - SEBAGH ; SCP MARLANGE, DE LA BURGADE ; SCP SPINOSI".to_string()));
        assert_eq!(
            pre_dila.rapporteur,
            Some("M. Alexandre Trémolière".to_string())
        );
        assert_eq!(
            pre_dila.government_commissioner,
            Some("M. Clément Malverti".to_string())
        );
        assert_eq!(
            pre_dila.ecli_code,
            Some("ECLI:FR:CECHR:2024:490536.20240321".to_string())
        );
    }
}
