use super::QueryTemplate;
use std::collections::HashMap;

use crate::config::Config;

pub struct QueryMatcher {
    templates: HashMap<String, QueryTemplate>,
}

impl QueryMatcher {
    pub fn new(config: &Config) -> Self {
        let mut templates = HashMap::new();
        for query in &config.queries {
            templates.insert(query.sql.clone(), query.clone());
        }
        QueryMatcher { templates }
    }

    pub fn find_template(&self, sql: &str) -> Option<&QueryTemplate> {
        self.templates.get(sql)
    }

    pub fn template_exists(&self, sql: &str) -> bool {
        self.templates.contains_key(sql)
    }
}
