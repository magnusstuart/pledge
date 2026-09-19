use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PostcardValue {
    Object(Vec<(String, PostcardValue)>), // Vec instead of HashMap as it's more efficient for small data sets and Postcard gets mad at HashMap
    Array(Vec<PostcardValue>),
    String(String),
    Integer8(i8),
    Integer16(i16),
    Integer32(i32),
    Integer64(i64),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    Null,
}
