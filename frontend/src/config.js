const rawMaxFileSizeMb = Number(import.meta.env.VITE_MAX_FILE_SIZE_MB)

export const MAX_FILE_SIZE_MB =
  Number.isFinite(rawMaxFileSizeMb) && rawMaxFileSizeMb > 0 ? Math.floor(rawMaxFileSizeMb) : 200

export const MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
