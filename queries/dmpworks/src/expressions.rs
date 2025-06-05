use std::collections::HashMap;

use polars::prelude::*;
use polars_core::series::Series;
use pyo3_polars::derive::polars_expr;
use serde_json;

#[polars_expr(output_type=String)]
fn revert_inverted_index(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
    let out: StringChunked = ca.apply_into_string_amortized(|value: &str, output: &mut String| {
        // Parse JSON
        let data: HashMap<String, Vec<u32>> = match serde_json::from_str(value) {
            Ok(data) => data,
            Err(_) => return, // Skip invalid JSON rows
        };

        // Revert index
        let mut words: Vec<Option<String>> = Vec::new();
        for (word, positions) in &data {
            for pos in positions {
                let index = *pos as usize;
                if words.len() <= index {
                    words.resize(index + 1, None)
                }
                words[index] = Some(word.clone());
            }
        }

        // Write to string buffer
        let mut iter = words.into_iter().filter_map(|x| x);
        if let Some(first) = iter.next() {
            output.push_str(&first);
            for word in iter {
                output.push(' ');
                output.push_str(&word);
            }
        }
    });
    Ok(out.into_series())
}

fn parse_datacite_affiliations_output(input_fields: &[Field]) -> PolarsResult<Field> {
    let name = Field::new("name".into(), DataType::String);
    let affiliation_identifier = Field::new("affiliationIdentifier".into(), DataType::String);
    let affiliation_identifier_scheme = Field::new("affiliationIdentifierScheme".into(), DataType::String);
    let scheme_uri = Field::new("schemeUri".into(), DataType::String);

    let v: Vec<Field> = vec![
        name,
        affiliation_identifier,
        affiliation_identifier_scheme,
        scheme_uri,
    ];
    Ok(Field::new(
        input_fields[0].name.clone(),
        DataType::List(Box::new(DataType::Struct(v))),
    ))
}

#[polars_expr(output_type_func=parse_datacite_affiliations_output)]
fn parse_datacite_affiliations(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;

    fn empty_struct_series() -> Series {
        let name_series = Series::new("name".into(), vec![None::<String>; 0]);
        let affiliation_identifier_series = Series::new("affiliationIdentifier".into(), vec![None::<String>; 0]);
        let affiliation_identifier_scheme_series = Series::new("affiliationIdentifierScheme".into(), vec![None::<String>; 0]);
        let scheme_uri_series = Series::new("schemeUri".into(), vec![None::<String>; 0]);

        let fields = vec![
            name_series,
            affiliation_identifier_series,
            affiliation_identifier_scheme_series,
            scheme_uri_series,
        ];

        StructChunked::from_series("".into(), 0, fields.iter()).unwrap().into_series()
    }

    let list_ca: ListChunked = ca
        .into_iter()
        .map(|opt_str| {
            // Skip nulls
            let s: &str = match opt_str {
                Some(s) => s,
                None => {
                    return Some(empty_struct_series());
                },
            };

            // Parse JSON
            // Convert single JSON objects into a vector of objects
            // Keep JSON arrays as arrays
            // Skip other cases
            let json_array: Vec<serde_json::Value> = match serde_json::from_str(s) {
                Ok(serde_json::Value::Object(obj)) => vec![serde_json::Value::Object(obj)],
                Ok(serde_json::Value::Array(arr)) => arr,
                Ok(_) => return Some(empty_struct_series()), // Skip unexpected JSON types
                Err(_) => return Some(empty_struct_series()), // Skip invalid JSON
            };

            // Build vectors
            let mut name = Vec::with_capacity(json_array.len());
            let mut affiliation_identifier = Vec::with_capacity(json_array.len());
            let mut affiliation_identifier_scheme = Vec::with_capacity(json_array.len());
            let mut scheme_uri = Vec::with_capacity(json_array.len());
            for obj in &json_array {
                name.push(obj.get("name").and_then(|v| v.as_str()).map(str::to_owned));
                affiliation_identifier.push(
                    obj.get("affiliationIdentifier")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                );
                affiliation_identifier_scheme.push(
                    obj.get("affiliationIdentifierScheme")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                );
                scheme_uri.push(
                    obj.get("schemeUri")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                );
            }

            // Build each series
            let name_series = Series::new("name".into(), name);
            let affiliation_identifier_series = Series::new("affiliationIdentifier".into(), affiliation_identifier);
            let affiliation_identifier_scheme_series = Series::new("affiliationIdentifierScheme".into(), affiliation_identifier_scheme);
            let scheme_uri_series = Series::new("schemeUri".into(), scheme_uri);
            let fields = vec![
                name_series,
                affiliation_identifier_series,
                affiliation_identifier_scheme_series,
                scheme_uri_series,
            ];

            // Build list of structs
            StructChunked::from_series("".into(), json_array.len(), fields.iter()).ok().map(|s| s.into_series())
        })
        .collect::<ListChunked>();

    Ok(list_ca.into_series())
}

fn parse_datacite_name_identifiers_output(input_fields: &[Field]) -> PolarsResult<Field> {
    let name_identifier = Field::new("nameIdentifier".into(), DataType::String);
    let name_identifier_scheme = Field::new("nameIdentifierScheme".into(), DataType::String);
    let scheme_uri = Field::new("schemeUri".into(), DataType::String);

    let v: Vec<Field> = vec![name_identifier, name_identifier_scheme, scheme_uri];
    Ok(Field::new(
        input_fields[0].name.clone(),
        DataType::List(Box::new(DataType::Struct(v))),
    ))
}

#[polars_expr(output_type_func=parse_datacite_name_identifiers_output)]
fn parse_datacite_name_identifiers(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;

    fn empty_struct_series() -> Series {
        let name_identifier_series = Series::new("nameIdentifier".into(), vec![None::<String>; 0]);
        let name_identifier_scheme_series = Series::new("nameIdentifierScheme".into(), vec![None::<String>; 0]);
        let scheme_uri_series = Series::new("schemeUri".into(), vec![None::<String>; 0]);

        let fields = vec![
            name_identifier_series,
            name_identifier_scheme_series,
            scheme_uri_series,
        ];

        StructChunked::from_series("".into(), 0, fields.iter()).unwrap().into_series()
    }

    let list_ca: ListChunked = ca
        .into_iter()
        .map(|opt_str| {
            // Skip nulls
            let s: &str = match opt_str {
                Some(s) => s,
                None => {
                    return Some(empty_struct_series());
                },
            };

            // Parse JSON
            // Convert single JSON objects into a vector of objects
            // Keep JSON arrays as arrays
            // Skip other cases
            let json_array: Vec<serde_json::Value> = match serde_json::from_str(s) {
                Ok(serde_json::Value::Object(obj)) => vec![serde_json::Value::Object(obj)],
                Ok(serde_json::Value::Array(arr)) => arr,
                Ok(_) => return Some(empty_struct_series()), // Skip unexpected JSON types
                Err(_) => return Some(empty_struct_series()), // Skip invalid JSON
            };

            // Build vectors
            let mut name_identifier = Vec::with_capacity(json_array.len());
            let mut name_identifier_scheme = Vec::with_capacity(json_array.len());
            let mut scheme_uri = Vec::with_capacity(json_array.len());
            for obj in &json_array {
                name_identifier.push(
                    obj.get("nameIdentifier")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                );
                name_identifier_scheme.push(
                    obj.get("nameIdentifierScheme")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                );
                scheme_uri.push(
                    obj.get("schemeUri")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned),
                );
            }

            // Build each series
            let name_identifier_series = Series::new("nameIdentifier".into(), name_identifier);
            let name_identifier_scheme_series = Series::new("nameIdentifierScheme".into(), name_identifier_scheme);
            let scheme_uri_series = Series::new("schemeUri".into(), scheme_uri);
            let fields = vec![
                name_identifier_series,
                name_identifier_scheme_series,
                scheme_uri_series,
            ];

            // Build list of structs
            StructChunked::from_series("".into(), json_array.len(), fields.iter()).ok().map(|s| s.into_series())
        })
        .collect::<ListChunked>();

    Ok(list_ca.into_series())
}
