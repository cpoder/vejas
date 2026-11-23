use postgres::{Client, NoTls, Row};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};

#[derive(Debug, Deserialize)]
#[serde(tag = "op")]
enum Request {
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "query")]
    Query { sql: String, params: Option<Vec<Value>> },
    #[serde(rename = "execute")]
    Execute { sql: String, params: Option<Vec<Value>> },
    #[serde(rename = "transaction")]
    Transaction { statements: Vec<Statement> },
}

#[derive(Debug, Deserialize)]
struct Statement {
    sql: String,
    params: Option<Vec<Value>>,
}

#[derive(Debug, Serialize)]
struct Response {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn row_to_json(row: &Row) -> Value {
    let mut map = serde_json::Map::new();
    for (idx, column) in row.columns().iter().enumerate() {
        let name = column.name();
        let value: Value = match column.type_().name() {
            "int2" | "int4" => row.get::<_, Option<i32>>(idx).map(|v| json!(v)).unwrap_or(Value::Null),
            "int8" => row.get::<_, Option<i64>>(idx).map(|v| json!(v)).unwrap_or(Value::Null),
            "float4" => row.get::<_, Option<f32>>(idx).map(|v| json!(v)).unwrap_or(Value::Null),
            "float8" => row.get::<_, Option<f64>>(idx).map(|v| json!(v)).unwrap_or(Value::Null),
            "bool" => row.get::<_, Option<bool>>(idx).map(|v| json!(v)).unwrap_or(Value::Null),
            "text" | "varchar" | "char" | "bpchar" => {
                row.get::<_, Option<String>>(idx).map(|v| json!(v)).unwrap_or(Value::Null)
            }
            "json" | "jsonb" => {
                row.get::<_, Option<Value>>(idx).unwrap_or(Value::Null)
            }
            _ => {
                // Fallback: try as string
                row.get::<_, Option<String>>(idx).map(|v| json!(v)).unwrap_or(Value::Null)
            }
        };
        map.insert(name.to_string(), value);
    }
    Value::Object(map)
}

fn json_to_param(value: &Value) -> Box<dyn postgres::types::ToSql + Sync> {
    match value {
        Value::Null => Box::new(None::<String>),
        Value::Bool(b) => Box::new(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Box::new(i)
            } else if let Some(f) = n.as_f64() {
                Box::new(f)
            } else {
                Box::new(n.to_string())
            }
        }
        Value::String(s) => Box::new(s.clone()),
        Value::Array(_) | Value::Object(_) => Box::new(value.clone()),
    }
}

fn handle_request(client: &mut Client, req: Request) -> Response {
    match req {
        Request::Ping => Response {
            ok: true,
            result: Some(json!({"pong": true})),
            error: None,
        },
        Request::Query { sql, params } => {
            let param_refs: Vec<Box<dyn postgres::types::ToSql + Sync>> = params
                .as_ref()
                .map(|p| p.iter().map(json_to_param).collect())
                .unwrap_or_default();
            
            let param_slice: Vec<&(dyn postgres::types::ToSql + Sync)> = 
                param_refs.iter().map(|b| b.as_ref()).collect();

            match client.query(&sql, &param_slice) {
                Ok(rows) => {
                    let result: Vec<Value> = rows.iter().map(row_to_json).collect();
                    Response {
                        ok: true,
                        result: Some(json!(result)),
                        error: None,
                    }
                }
                Err(e) => Response {
                    ok: false,
                    result: None,
                    error: Some(format!("query error: {}", e)),
                },
            }
        }
        Request::Execute { sql, params } => {
            let param_refs: Vec<Box<dyn postgres::types::ToSql + Sync>> = params
                .as_ref()
                .map(|p| p.iter().map(json_to_param).collect())
                .unwrap_or_default();
            
            let param_slice: Vec<&(dyn postgres::types::ToSql + Sync)> = 
                param_refs.iter().map(|b| b.as_ref()).collect();

            match client.execute(&sql, &param_slice) {
                Ok(affected) => Response {
                    ok: true,
                    result: Some(json!({"affected_rows": affected})),
                    error: None,
                },
                Err(e) => Response {
                    ok: false,
                    result: None,
                    error: Some(format!("execute error: {}", e)),
                },
            }
        }
        Request::Transaction { statements } => {
            let mut tx = match client.transaction() {
                Ok(t) => t,
                Err(e) => {
                    return Response {
                        ok: false,
                        result: None,
                        error: Some(format!("transaction start error: {}", e)),
                    }
                }
            };

            let mut results = Vec::new();
            for stmt in statements {
                let param_refs: Vec<Box<dyn postgres::types::ToSql + Sync>> = stmt.params
                    .as_ref()
                    .map(|p| p.iter().map(json_to_param).collect())
                    .unwrap_or_default();
                
                let param_slice: Vec<&(dyn postgres::types::ToSql + Sync)> = 
                    param_refs.iter().map(|b| b.as_ref()).collect();

                match tx.execute(&stmt.sql, &param_slice) {
                    Ok(affected) => results.push(json!({"affected_rows": affected})),
                    Err(e) => {
                        let _ = tx.rollback();
                        return Response {
                            ok: false,
                            result: None,
                            error: Some(format!("transaction error: {}", e)),
                        };
                    }
                }
            }

            match tx.commit() {
                Ok(_) => Response {
                    ok: true,
                    result: Some(json!({"results": results})),
                    error: None,
                },
                Err(e) => Response {
                    ok: false,
                    result: None,
                    error: Some(format!("transaction commit error: {}", e)),
                },
            }
        }
    }
}

fn main() {
    // Get connection string from environment
    let conn_str = std::env::var("POSTGRES_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| {
            eprintln!("ERROR: POSTGRES_URL or DATABASE_URL environment variable required");
            std::process::exit(1);
        });

    // Connect to PostgreSQL
    let mut client = match Client::connect(&conn_str, NoTls) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: Failed to connect to PostgreSQL: {}", e);
            std::process::exit(1);
        }
    };

    // Signal readiness
    println!("{{\"ready\":true}}");
    io::stdout().flush().unwrap();

    // Process requests line by line
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("ERROR: Failed to read line: {}", e);
                continue;
            }
        };

        let request: Request = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                let response = Response {
                    ok: false,
                    result: None,
                    error: Some(format!("invalid request JSON: {}", e)),
                };
                println!("{}", serde_json::to_string(&response).unwrap());
                io::stdout().flush().unwrap();
                continue;
            }
        };

        let response = handle_request(&mut client, request);
        println!("{}", serde_json::to_string(&response).unwrap());
        io::stdout().flush().unwrap();
    }
}
