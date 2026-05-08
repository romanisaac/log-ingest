import type { NextConfig } from 'next'

const isDev = process.env.NODE_ENV === 'development'

const BACKEND = process.env.NEXT_PUBLIC_BACKEND_URL ?? 'http://localhost:8080'

const nextConfig: NextConfig = {
  // Static export for production (served from the Rust server at the same origin).
  // Omitted in dev so that rewrites work.
  ...(!isDev ? { output: 'export' } : {}),
  trailingSlash: true,
  images: { unoptimized: true },
  ...(isDev
    ? {
        async rewrites() {
          return [
            { source: '/query',         destination: `${BACKEND}/query` },
            { source: '/jobs',          destination: `${BACKEND}/jobs` },
            { source: '/jobs/:path*',   destination: `${BACKEND}/jobs/:path*` },
            { source: '/ingest',        destination: `${BACKEND}/ingest` },
            { source: '/metrics',       destination: `${BACKEND}/metrics` },
            { source: '/health',        destination: `${BACKEND}/health` },
            { source: '/ready',         destination: `${BACKEND}/ready` },
          ]
        },
      }
    : {}),
}

export default nextConfig
