-- Backend table for the field-encryption seam tests (MC-427): content
-- columns (`title`, `amount`) are nullable so enforced-mode pushes can null
-- them, and the row carries one AEAD blob (`content_enc`) plus the circle key
-- version it was encrypted under (`key_version`). The plaintext envelope
-- (id, user_id, updated_at, deleted, circle_id, assignee) keeps working for
-- conflict resolution, RLS, and FK-style references.
create table
secret_items (
    id uuid not null,
    user_id uuid not null references auth.users (id) on delete cascade,
    updated_at timestamptz not null,
    deleted boolean not null,
    circle_id uuid,
    title text,
    amount integer,
    assignee text,
    content_enc text,
    key_version integer,
    primary key (id)
);

create trigger handle_conflicts
before update on secret_items
for each row
execute function discard_older_updates();

alter publication supabase_realtime add table secret_items;

alter table secret_items enable row level security;

create policy "Users can work with own data"  -- noqa
on public.secret_items
for all
using (
    (auth.uid() = user_id)
);
