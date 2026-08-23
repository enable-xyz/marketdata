-- +goose Up
-- PostgreSQL timestamptz stores microseconds. Mapper selection is defined on
-- capture receive-wall nanoseconds, so retain the UTC timestamps for inspection
-- and make these bigint coordinates the canonical interval identity.
ALTER TABLE mapper_binding
    ADD COLUMN effective_from_ns bigint,
    ADD COLUMN effective_to_ns bigint;

UPDATE mapper_binding
SET effective_from_ns = (extract(epoch FROM effective_from) * 1000000000)::bigint,
    effective_to_ns = CASE
        WHEN effective_to IS NULL THEN NULL
        ELSE (extract(epoch FROM effective_to) * 1000000000)::bigint
    END;

ALTER TABLE mapper_binding DROP CONSTRAINT mapper_binding_check;

ALTER TABLE mapper_binding
    ALTER COLUMN effective_from_ns SET NOT NULL,
    ADD CONSTRAINT mapper_binding_effective_from_ns_nonnegative CHECK (effective_from_ns >= 0),
    ADD CONSTRAINT mapper_binding_effective_ns_ordered
        CHECK (effective_to_ns IS NULL OR effective_to_ns > effective_from_ns),
    ADD CONSTRAINT mapper_binding_effective_from_projection CHECK (
        effective_from = TIMESTAMPTZ 'epoch'
            + (effective_from_ns / 1000000000) * INTERVAL '1 second'
            + ((effective_from_ns % 1000000000) / 1000) * INTERVAL '1 microsecond'
    ),
    ADD CONSTRAINT mapper_binding_effective_to_projection CHECK (
        (effective_to IS NULL) = (effective_to_ns IS NULL) AND (
            effective_to IS NULL OR effective_to = TIMESTAMPTZ 'epoch'
                + (effective_to_ns / 1000000000) * INTERVAL '1 second'
                + ((effective_to_ns % 1000000000) / 1000) * INTERVAL '1 microsecond'
        )
    ),
    ADD CONSTRAINT mapper_binding_schema_fingerprints_nonempty
        CHECK (jsonb_array_length(schema_fingerprints) BETWEEN 1 AND 256);

ALTER TABLE mapper_binding
    DROP CONSTRAINT mapper_binding_pkey,
    DROP CONSTRAINT mapper_binding_source_id_channel_id_tstzrange_excl;

ALTER TABLE mapper_binding
    ADD PRIMARY KEY (source_id, channel_id, effective_from_ns),
    ADD CONSTRAINT mapper_binding_receive_ns_excl EXCLUDE USING gist (
        source_id WITH =,
        channel_id WITH =,
        int8range(effective_from_ns, effective_to_ns, '[)') WITH &&
    );

DROP TRIGGER mapper_binding_identity_immutable ON mapper_binding;
CREATE TRIGGER mapper_binding_identity_immutable
BEFORE UPDATE OF source_id, channel_id, effective_from, effective_from_ns ON mapper_binding
FOR EACH ROW EXECUTE FUNCTION catalog_reject_identity_update(
    'source_id', 'channel_id', 'effective_from', 'effective_from_ns'
);

ALTER TABLE mapper_release
    ADD CONSTRAINT mapper_release_mapper_version_nonblank CHECK (btrim(mapper_version) <> ''),
    ADD CONSTRAINT mapper_release_build_identity_nonblank CHECK (btrim(build_identity) <> ''),
    ADD CONSTRAINT mapper_release_schema_version_nonblank CHECK (btrim(normalized_schema_version) <> ''),
    ADD CONSTRAINT mapper_release_source_identity_nonblank CHECK (btrim(source_code_identity) <> ''),
    ADD CONSTRAINT mapper_release_fixture_hash_nonzero
        CHECK (fixture_bundle_hash <> decode(repeat('00', 32), 'hex'));

ALTER TABLE mapper_binding
    ADD CONSTRAINT mapper_binding_channel_nonblank CHECK (btrim(channel_id) <> '');

-- +goose StatementBegin
CREATE FUNCTION catalog_mapper_run_evidence_complete(run jsonb)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN jsonb_typeof(run) IS DISTINCT FROM 'object' THEN false
        WHEN jsonb_typeof(run -> 'accepted_fields') IS DISTINCT FROM 'array' THEN false
        WHEN jsonb_array_length(run -> 'accepted_fields') > 400000 THEN false
        WHEN EXISTS (
            SELECT 1 FROM jsonb_array_elements(run -> 'accepted_fields') AS accepted(value)
            WHERE jsonb_typeof(accepted.value) IS DISTINCT FROM 'string'
                OR btrim(accepted.value #>> '{}') = ''
                OR octet_length(accepted.value #>> '{}') > 4096
        ) THEN false
        WHEN (
            SELECT count(*) FROM jsonb_array_elements(run -> 'accepted_fields')
        ) <> (
            SELECT count(DISTINCT accepted.value #>> '{}')
            FROM jsonb_array_elements(run -> 'accepted_fields') AS accepted(value)
        ) THEN false
        WHEN EXISTS (
            SELECT 1
            FROM (
                SELECT accepted.value #>> '{}' AS current_value,
                    lag(accepted.value #>> '{}') OVER (ORDER BY accepted.ordinality) AS previous_value
                FROM jsonb_array_elements(run -> 'accepted_fields')
                    WITH ORDINALITY AS accepted(value, ordinality)
            ) AS ordered
            WHERE ordered.previous_value IS NOT NULL
                AND ordered.current_value COLLATE "C" <= ordered.previous_value COLLATE "C"
        ) THEN false
        WHEN jsonb_typeof(run -> 'rejection_counts') IS DISTINCT FROM 'object' THEN false
        WHEN (
            SELECT count(*) FROM jsonb_object_keys(run -> 'rejection_counts')
        ) > 256 THEN false
        WHEN EXISTS (
            SELECT 1 FROM jsonb_each(run -> 'rejection_counts') AS item
            WHERE btrim(item.key) = '' OR octet_length(item.key) > 4096
                OR jsonb_typeof(item.value) IS DISTINCT FROM 'number'
                OR (item.value #>> '{}') !~ '^[1-9][0-9]*$'
        ) THEN false
        WHEN EXISTS (
            SELECT 1 FROM jsonb_each(run -> 'rejection_counts') AS item
            WHERE (item.value #>> '{}')::numeric > 18446744073709551615
        ) THEN false
        WHEN coalesce(run ->> 'logical_sha256', '') !~ '^[0-9a-f]{64}$' THEN false
        WHEN run ->> 'logical_sha256' = repeat('0', 64) THEN false
        WHEN coalesce(run ->> 'semantic_sha256', '') !~ '^[0-9a-f]{64}$' THEN false
        WHEN run ->> 'semantic_sha256' = repeat('0', 64) THEN false
        WHEN jsonb_typeof(run -> 'rejections') IS DISTINCT FROM 'array' THEN false
        WHEN jsonb_array_length(run -> 'rejections') > 400000 THEN false
        WHEN EXISTS (
            SELECT 1 FROM jsonb_array_elements(run -> 'rejections') AS rejection(value)
            WHERE jsonb_typeof(rejection.value) IS DISTINCT FROM 'string'
                OR btrim(rejection.value #>> '{}') = ''
                OR octet_length(rejection.value #>> '{}') > 32768
        ) THEN false
        WHEN coalesce(run ->> 'rejection_sha256', '') !~ '^[0-9a-f]{64}$' THEN false
        WHEN run ->> 'rejection_sha256' = repeat('0', 64) THEN false
        WHEN btrim(coalesce(run ->> 'downstream_book_result', '')) = '' THEN false
        WHEN octet_length(run ->> 'downstream_book_result') > 4096 THEN false
        WHEN coalesce(run ->> 'downstream_book_sha256', '') !~ '^[0-9a-f]{64}$' THEN false
        WHEN run ->> 'downstream_book_sha256' = repeat('0', 64) THEN false
        ELSE true
    END
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION catalog_mapper_schema_fingerprints_complete(fingerprints jsonb)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN jsonb_typeof(fingerprints) IS DISTINCT FROM 'array' THEN false
        WHEN jsonb_array_length(fingerprints) NOT BETWEEN 1 AND 256 THEN false
        WHEN EXISTS (
            SELECT 1 FROM jsonb_array_elements(fingerprints) AS fingerprint(value)
            WHERE jsonb_typeof(fingerprint.value) IS DISTINCT FROM 'object'
                OR btrim(coalesce(fingerprint.value ->> 'name', '')) = ''
                OR fingerprint.value ->> 'version' IS DISTINCT FROM '1'
                OR fingerprint.value ->> 'logical_encoding_version' IS DISTINCT FROM '1'
                OR coalesce(fingerprint.value ->> 'sha256', '') !~ '^[0-9a-f]{64}$'
                OR fingerprint.value ->> 'sha256' = repeat('0', 64)
        ) THEN false
        WHEN (
            SELECT count(*) FROM jsonb_array_elements(fingerprints)
        ) <> (
            SELECT count(DISTINCT jsonb_build_array(
                fingerprint.value ->> 'name', fingerprint.value ->> 'version'
            ))
            FROM jsonb_array_elements(fingerprints) AS fingerprint(value)
        ) THEN false
        ELSE true
    END
$$;
-- +goose StatementEnd

ALTER TABLE mapper_binding
    ADD CONSTRAINT mapper_binding_schema_fingerprints_complete
        CHECK (catalog_mapper_schema_fingerprints_complete(schema_fingerprints));

-- +goose StatementBegin
CREATE FUNCTION catalog_mapper_evidence_complete(evidence jsonb)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN jsonb_typeof(evidence) IS DISTINCT FROM 'object' THEN false
        WHEN octet_length(evidence::text) > 134217728 THEN false
        WHEN evidence ->> 'version' IS DISTINCT FROM '1' THEN false
        WHEN coalesce(evidence ->> 'source_id', '') !~
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN false
        WHEN btrim(coalesce(evidence ->> 'channel_id', '')) = ''
            OR octet_length(evidence ->> 'channel_id') > 4096 THEN false
        WHEN evidence ->> 'selection_time_basis' IS DISTINCT FROM 'received_wall_time_ns' THEN false
        WHEN jsonb_typeof(evidence -> 'received_start_ns') IS DISTINCT FROM 'number'
            OR coalesce(evidence ->> 'received_start_ns', '') !~ '^[0-9]+$'
            OR (evidence ->> 'received_start_ns')::numeric > 9223372036854775807 THEN false
        WHEN jsonb_typeof(evidence -> 'received_end_ns') IS DISTINCT FROM 'number'
            OR coalesce(evidence ->> 'received_end_ns', '') !~ '^[0-9]+$'
            OR (evidence ->> 'received_end_ns')::numeric > 9223372036854775807 THEN false
        WHEN (evidence ->> 'received_end_ns')::numeric < (evidence ->> 'received_start_ns')::numeric THEN false
        WHEN jsonb_typeof(evidence -> 'corpus_count') IS DISTINCT FROM 'number'
            OR coalesce(evidence ->> 'corpus_count', '') !~ '^[1-9][0-9]*$'
            OR (evidence ->> 'corpus_count')::numeric > 18446744073709551615 THEN false
        WHEN coalesce(evidence ->> 'corpus_sha256', '') !~ '^[0-9a-f]{64}$'
            OR evidence ->> 'corpus_sha256' = repeat('0', 64) THEN false
        WHEN coalesce(evidence ->> 'old_mapper_release_id', '') !~
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN false
        WHEN coalesce(evidence ->> 'new_mapper_release_id', '') !~
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN false
        WHEN evidence ->> 'old_mapper_release_id' = evidence ->> 'new_mapper_release_id' THEN false
        WHEN catalog_mapper_run_evidence_complete(evidence -> 'old') IS DISTINCT FROM true THEN false
        WHEN catalog_mapper_run_evidence_complete(evidence -> 'new') IS DISTINCT FROM true THEN false
        WHEN jsonb_typeof(evidence -> 'mismatch_codes') IS DISTINCT FROM 'array'
            OR jsonb_array_length(evidence -> 'mismatch_codes') > 256 THEN false
        WHEN EXISTS (
            SELECT 1 FROM jsonb_array_elements(evidence -> 'mismatch_codes') AS mismatch(value)
            WHERE jsonb_typeof(mismatch.value) IS DISTINCT FROM 'string'
                OR btrim(mismatch.value #>> '{}') = ''
                OR octet_length(mismatch.value #>> '{}') > 4096
        ) THEN false
        WHEN (
            SELECT count(*) FROM jsonb_array_elements(evidence -> 'mismatch_codes')
        ) <> (
            SELECT count(DISTINCT mismatch.value #>> '{}')
            FROM jsonb_array_elements(evidence -> 'mismatch_codes') AS mismatch(value)
        ) THEN false
        WHEN jsonb_typeof(evidence -> 'mismatch') IS DISTINCT FROM 'boolean' THEN false
        WHEN (evidence ->> 'mismatch')::boolean IS DISTINCT FROM (
            evidence -> 'old' -> 'accepted_fields' IS DISTINCT FROM evidence -> 'new' -> 'accepted_fields'
            OR evidence -> 'old' -> 'rejection_counts' IS DISTINCT FROM evidence -> 'new' -> 'rejection_counts'
            OR evidence -> 'old' ->> 'semantic_sha256' IS DISTINCT FROM evidence -> 'new' ->> 'semantic_sha256'
            OR evidence -> 'old' -> 'rejections' IS DISTINCT FROM evidence -> 'new' -> 'rejections'
            OR evidence -> 'old' ->> 'rejection_sha256' IS DISTINCT FROM evidence -> 'new' ->> 'rejection_sha256'
            OR evidence -> 'old' ->> 'downstream_book_result' IS DISTINCT FROM evidence -> 'new' ->> 'downstream_book_result'
            OR evidence -> 'old' ->> 'downstream_book_sha256' IS DISTINCT FROM evidence -> 'new' ->> 'downstream_book_sha256'
        ) THEN false
        WHEN (evidence ->> 'mismatch')::boolean IS DISTINCT FROM
            (jsonb_array_length(evidence -> 'mismatch_codes') <> 0) THEN false
        WHEN evidence ->> 'decision' IS DISTINCT FROM 'accepted'
            AND evidence ->> 'decision' IS DISTINCT FROM 'rejected' THEN false
        WHEN evidence ->> 'decision' = 'accepted' AND (evidence ->> 'mismatch')::boolean THEN false
        WHEN evidence ->> 'decision' = 'rejected' AND NOT (evidence ->> 'mismatch')::boolean THEN false
        ELSE true
    END
$$;
-- +goose StatementEnd

ALTER TABLE mapper_binding
    ADD CONSTRAINT mapper_binding_evidence_complete CHECK (
        dual_run_evidence = '{}'::jsonb OR catalog_mapper_evidence_complete(dual_run_evidence)
    );

-- +goose StatementBegin
CREATE FUNCTION catalog_mapper_evidence_publishable(
    evidence jsonb,
    binding_source_id uuid,
    binding_channel_id text,
    binding_release_id uuid
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN catalog_mapper_evidence_complete(evidence) IS DISTINCT FROM true THEN false
        WHEN evidence ->> 'source_id' IS DISTINCT FROM binding_source_id::text THEN false
        WHEN evidence ->> 'channel_id' IS DISTINCT FROM binding_channel_id THEN false
        WHEN evidence ->> 'new_mapper_release_id' IS DISTINCT FROM binding_release_id::text THEN false
        WHEN evidence -> 'old' -> 'accepted_fields' IS DISTINCT FROM
            evidence -> 'new' -> 'accepted_fields' THEN false
        WHEN evidence -> 'old' -> 'rejection_counts' IS DISTINCT FROM
            evidence -> 'new' -> 'rejection_counts' THEN false
        WHEN evidence -> 'old' ->> 'semantic_sha256' IS DISTINCT FROM
            evidence -> 'new' ->> 'semantic_sha256' THEN false
        WHEN evidence -> 'old' -> 'rejections' IS DISTINCT FROM
            evidence -> 'new' -> 'rejections' THEN false
        WHEN evidence -> 'old' ->> 'rejection_sha256' IS DISTINCT FROM
            evidence -> 'new' ->> 'rejection_sha256' THEN false
        WHEN evidence -> 'old' ->> 'downstream_book_result' IS DISTINCT FROM
            evidence -> 'new' ->> 'downstream_book_result' THEN false
        WHEN evidence -> 'old' ->> 'downstream_book_sha256' IS DISTINCT FROM
            evidence -> 'new' ->> 'downstream_book_sha256' THEN false
        WHEN jsonb_array_length(evidence -> 'mismatch_codes') <> 0 THEN false
        WHEN (evidence ->> 'mismatch')::boolean THEN false
        WHEN evidence ->> 'decision' IS DISTINCT FROM 'accepted' THEN false
        ELSE true
    END
$$;
-- +goose StatementEnd

ALTER TABLE mapper_binding
    ADD CONSTRAINT mapper_binding_active_evidence_publishable CHECK (
        state NOT IN ('active', 'retired') OR
        catalog_mapper_evidence_publishable(
            dual_run_evidence, source_id, channel_id, mapper_release_id
        )
    );

-- The final evidence object and mapper/schema identities are immutable once a
-- binding is selectable. An active binding may only close its upper boundary
-- and become retired during an atomic cutover.
-- +goose StatementBegin
CREATE FUNCTION catalog_guard_mapper_binding_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.state IN ('active', 'retired') AND (
        OLD.source_id IS DISTINCT FROM NEW.source_id OR
        OLD.channel_id IS DISTINCT FROM NEW.channel_id OR
        OLD.effective_from IS DISTINCT FROM NEW.effective_from OR
        OLD.effective_from_ns IS DISTINCT FROM NEW.effective_from_ns OR
        OLD.mapper_release_id IS DISTINCT FROM NEW.mapper_release_id OR
        OLD.schema_fingerprints IS DISTINCT FROM NEW.schema_fingerprints OR
        OLD.dual_run_evidence IS DISTINCT FROM NEW.dual_run_evidence
    ) THEN
        RAISE EXCEPTION 'selectable mapper binding identity and evidence are immutable'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.state = 'retired' AND NEW.state <> 'retired' THEN
        RAISE EXCEPTION 'retired mapper binding state is terminal'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.state = 'active' AND NEW.state NOT IN ('active', 'retired') THEN
        RAISE EXCEPTION 'active mapper binding may only remain active or retire'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER mapper_binding_update_guard
BEFORE UPDATE ON mapper_binding
FOR EACH ROW EXECUTE FUNCTION catalog_guard_mapper_binding_update();

-- JSON cannot carry a foreign key, so enforce the old release reference at the
-- same durable boundary as active/retired evidence publication.
-- +goose StatementBegin
CREATE FUNCTION catalog_validate_mapper_binding_release_evidence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.state IN ('active', 'retired') AND NOT EXISTS (
        SELECT 1 FROM mapper_release
        WHERE mapper_release_id::text = NEW.dual_run_evidence ->> 'old_mapper_release_id'
    ) THEN
        RAISE EXCEPTION 'dual-run old mapper release is absent'
            USING ERRCODE = 'foreign_key_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER mapper_binding_release_evidence_fk
BEFORE INSERT OR UPDATE OF dual_run_evidence, state ON mapper_binding
FOR EACH ROW EXECUTE FUNCTION catalog_validate_mapper_binding_release_evidence();
