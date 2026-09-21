-- Exact search-plane reconciliation pages the live catalog by passage id.
-- The concurrent companion supplies the ordered access path without blocking
-- passage writes while a large production catalog is indexed.
INSERT INTO schema_migrations(version) VALUES (70) ON CONFLICT DO NOTHING;
