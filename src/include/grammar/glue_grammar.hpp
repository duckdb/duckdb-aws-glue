#pragma once

#include "duckdb/parser/grammar_extension.hpp"

namespace duckdb {
class DatabaseInstance;

//! The 'glue_hive_ddl' grammar extension: Hive partition DDL as SQL
//!
//!   ALTER TABLE t ADD [IF NOT EXISTS] PARTITION (k = v, ...) [LOCATION '...'] [PARTITION (...) [LOCATION '...']]...
//!   ALTER TABLE t DROP [IF EXISTS] PARTITION (k = v, ...) [, PARTITION (...)]...
//!   ALTER TABLE t PARTITION (k = v, ...) RENAME TO PARTITION (k = v, ...)
//!   ALTER TABLE t PARTITION (k = v, ...) SET LOCATION '...'
//!   ALTER TABLE t SET LOCATION '...'
//!   MSCK REPAIR TABLE t
//!
//! Several of these actions can follow each other in one statement. The statement is turned into
//! CALL glue_alter_table(t, [actions]); MSCK REPAIR TABLE becomes CALL glue_repair_table(t). Activate it
//! with SET active_grammar_extensions = ['glue_hive_ddl'].
void RegisterGlueGrammarExtension(DatabaseInstance &db);

} // namespace duckdb
