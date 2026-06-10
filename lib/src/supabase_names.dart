const publicSchema = 'public';

const idKey = 'id';
const userIdKey = 'user_id';
const updatedAtKey = 'updated_at';
const deletedKey = 'deleted';

/// Wire keys used by the field-encryption seam. Tables registered for
/// encryption carry one AEAD blob per row in [contentEncKey], encrypted under
/// the circle key version in [keyVersionKey]; rows are scoped to a circle via
/// [circleIdKey] (which must stay plaintext — RLS and key resolution depend
/// on it).
const circleIdKey = 'circle_id';
const contentEncKey = 'content_enc';
const keyVersionKey = 'key_version';
