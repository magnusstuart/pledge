use std::time::Instant;

use crate::database::value::PostcardValue;
use crate::metrics::{CACHE_HITS, CACHE_MISSES, TOTAL_QUERY_DURATION, register_cache_hit};
use crate::server::state::AppState;
use crate::{cache::store::cache_key, database::conversion};
use axum::Json;
use axum::http::StatusCode;
use axum::response::Response;
use serde::{Deserialize, Serialize};
use sqlx::{Column, PgPool, Row};

#[derive(Deserialize)]
pub struct QueryRequest {
    sql: String,
    params: Vec<serde_json::Value>,
}

#[derive(Serialize, Deserialize)]
pub struct QueryResponse {
    rows: Vec<PostcardValue>,
}

pub async fn query_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(body): Json<QueryRequest>,
) -> Result<Response, (StatusCode, String)> {
    let start = Instant::now();
    println!("Received query: {:}", body.sql);
    println!("Params: {:?}", body.params);

    let matched_template = state.matcher.find_template(&body.sql);
    let key = cache_key(&body.sql, &body.params);

    if matched_template.is_some() {
        if let Some(cached_result) = state.cache.get(&key) {
            CACHE_HITS.inc();
            // let query_response =
            //     postcard::from_bytes::<QueryResponse>(&cached_result).map_err(|e| {
            //         (
            //             StatusCode::INTERNAL_SERVER_ERROR,
            //             format!("Postcard error: {}", e),
            //         )
            //     })?;
            // let json_value = response_to_json(&query_response);
            // let json_bytes = serde_json::to_vec(&json_value)
            //     .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            // let response = Response::builder()
            //     .header(header::CONTENT_TYPE, "application/json")
            //     .body(json_bytes.into())
            //     .unwrap();
            println!("✓ CACHE HIT (key: {})", &key[0..8]);
            TOTAL_QUERY_DURATION.observe(start.elapsed().as_secs_f64());
            register_cache_hit(start.elapsed());
            println!("Cache hit duration: {}", start.elapsed().as_secs_f64());
            //return Ok(response);
        }
    }

    // Cache miss path
    CACHE_MISSES.inc();
    println!("x CACHE MISS (key: {})", &key[0..8]);
    //let rows = execute_query(&state.pool, &body.sql, &body.params).await?;

    //let response = QueryResponse { rows };
    //let cache_bytes = postcard::to_allocvec(&response)
    //    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    // let json_value = response_to_json(&response);
    // let json_bytes = serde_json::to_vec(&json_value)
    //     .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // if let Some(template) = matched_template {
    //     let expiration = match template.ttl {
    //         Some(ttl) => Instant::now() + Duration::from_secs(ttl),
    //         None => Instant::now() + Duration::from_secs(state.global_ttl),
    //     };
    //     println!("[_] Stored in cache: {}", key);
    //     state.cache.insert(key, cache_bytes, expiration);
    // }

    TOTAL_QUERY_DURATION.observe(start.elapsed().as_secs_f64());
    // Ok(Response::builder()
    //     .header(header::CONTENT_TYPE, "application/json")
    //     .body(json_bytes.into())
    //     .unwrap())
    Err((StatusCode::INTERNAL_SERVER_ERROR, "asd".to_string()))
}

async fn execute_query(
    pool: &PgPool,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<Vec<PostcardValue>, (axum::http::StatusCode, String)> {
    let mut query = sqlx::query(sql);

    for param in params {
        // Bind each parameter
        if let Some(num) = param.as_i64() {
            query = query.bind(num);
        } else if let Some(text) = param.as_str() {
            query = query.bind(text);
        } else if let Some(bool) = param.as_bool() {
            query = query.bind(bool);
        } else if let Some(array) = param.as_array() {
            query = query.bind(array);
        } else if let Some(num) = param.as_f64() {
            query = query.bind(num);
        } else {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "Unsupported parameter type".to_string(),
            ));
        }
    }

    let rows = query
        .fetch_all(pool)
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let rows_to_return: Vec<PostcardValue> = rows
        .into_iter()
        .map(|row| {
            let mut fields: Vec<(String, PostcardValue)> = Vec::new();
            for (i, column) in row.columns().iter().enumerate() {
                let value = conversion::convert_row_val_to_postcard(&row, column, i);
                fields.push((column.name().to_string(), value));
            }
            PostcardValue::Object(fields)
        })
        .collect();

    Ok(rows_to_return)
}

fn response_to_json(response: &QueryResponse) -> serde_json::Value {
    serde_json::json!({
        "rows": response.rows.iter().map(postcard_to_json).collect::<Vec<_>>()
    })
}

fn postcard_to_json(val: &PostcardValue) -> serde_json::Value {
    match val {
        PostcardValue::Object(fields) => {
            let mut map = serde_json::Map::new();
            for (k, v) in fields {
                map.insert(k.clone(), postcard_to_json(v));
            }
            serde_json::Value::Object(map)
        }
        PostcardValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(postcard_to_json).collect())
        }
        PostcardValue::String(s) => serde_json::Value::String(s.clone()),
        PostcardValue::Integer8(i) => serde_json::json!(*i),
        PostcardValue::Integer16(i) => serde_json::json!(*i),
        PostcardValue::Integer32(i) => serde_json::json!(*i),
        PostcardValue::Integer64(i) => serde_json::json!(*i),
        PostcardValue::Float32(f) => serde_json::json!(*f),
        PostcardValue::Float64(f) => serde_json::json!(*f),
        PostcardValue::Bool(b) => serde_json::Value::Bool(*b),
        PostcardValue::Null => serde_json::Value::Null,
    }
}
