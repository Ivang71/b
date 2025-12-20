## Base
- **JSON** over HTTPS
- **Language**: `?lang=xx` or `Accept-Language` (fallback to `en`)

## Shared: TitleCard
- `id` (int)
- `kind` (`movie` | `series`)
- `name` (localized if available)
- `description` (string | null) — localized overview if available
- `year` (int | null)
- `rating` (number | null)
- `poster` (string | null) — TMDB poster path
- `backdrop` (string | null) — TMDB backdrop path
- `logo` (string | null) — local logo path if available, else TMDB logo, else `poster`

## Endpoints

### `GET /ping` / `GET /health`
Returns `ok`.

### `GET /v1/home`
Home payload, cached per language (~1.5h).

Returns:
- `as_of` (unix seconds)
- `providers` (fixed list)
- `slider` (`TitleCard[]`) — random 10 from TMDB trending/day
- `top10_today` (`TitleCard[]`) — random 10 from TMDB trending/day
- `trending_today` (`TitleCard[]`) — TMDB trending/week page 1
- `series_on` (object: provider -> `TitleCard[]`)
- `top_rated` `{ movies: TitleCard[], series: TitleCard[] }`
- `genres` (object: genre -> `TitleCard[]`)

### `GET /v1/titles/{id}`
Movie/series details (localized).

Returns:
- `id`, `kind`, `name`, `description`, `tags`, `year`, `runtime_min`, `rating`
- `poster`, `logo`, `backdrop`
- `trailer_youtube` (`{ key, url } | null`)
- `seasons` (`[{ season, episode_count }]`) (series only)
- `prefetch_season` (int | null) (series only)
- `prefetch_episodes` (`[{ episode, name, runtime_min, still }]`) (series only)
- `cast` (`[{ name, role, order, profile }]`)
- `similar` (`TitleCard[]`) (cached ~3 days)

### `GET /v1/browse/{tab}/{page}`
Endless browse list.

Returns:
- `tab`, `page`, `page_size`, `has_more`
- `items` (`TitleCard[]`)

### `GET /v1/search`
Search page bootstrap.

Returns:
- `trending_today` (`TitleCard[]`)
- `query` (`""`)
- `results` (`[]`)

### `GET /v1/search/{query}`
Search results.

Returns:
- `query`
- `results` (`TitleCard[]`) (max 12)

