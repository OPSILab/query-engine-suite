# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
# Debian-based (glibc), deliberately NOT Alpine: @angular/build - pulled in by
# @angular-devkit/build-angular, which runs the build - depends on lmdb, and
# lmdb publishes prebuilt binaries for glibc only (@lmdb/lmdb-linux-x64; there
# is no -musl package). On Alpine npm falls back to compiling it from source,
# which means adding python3/make/g++ to the image and hoping it builds. The
# builder stage is thrown away at the end anyway, so Debian costs nothing in
# the final image - only Docker's build cache.
#
# Node 24: Angular 21 declares `engines.node: ^20.19.0 || ^22.12.0 || >=24.0.0`.
# Note the gaps - plain "node:20" or "node:22" tags can resolve to a patch
# below those floors, which is why this pins the major that has no floor.
FROM node:24-slim AS builder

WORKDIR /app

# Copied on their own first, so this layer stays cached until the lockfile
# actually changes - `COPY . .` below would otherwise invalidate the install
# on every source edit.
COPY package.json package-lock.json .npmrc ./

# `npm ci`, not `npm install`: installs exactly what package-lock.json pins and
# fails if package.json and the lockfile have drifted apart, which is what you
# want in a build. (The lockfile is lockfileVersion 3 - needs npm 7+, which is
# bundled with every Node version this image can be.)
RUN npm ci
RUN npm install-scripts ls

COPY . .

# Set by docker-compose.yml (/query-engine-frontend/, the reverse proxy's
# sub-path - see the comment there on why the proxy must strip it). The "/"
# default only applies to a bare `docker build` without --build-arg.
#
# Getting this wrong fails in a confusing way rather than loudly: with a
# sub-path base href but nginx serving from the root, the browser asks for
# /query-engine-frontend/main.<hash>.js, nginx's `try_files ... /index.html`
# finds no such file and serves index.html instead, and the app dies on a
# script that was delivered as HTML.
ARG BASE_HREF=/
RUN npm run build:prod -- --base-href "${BASE_HREF}"

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
# Alpine is fine here - there's no Node in this stage, just nginx serving
# static files, so the musl problem above doesn't apply.
FROM nginx:stable-alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf

# The `browser` builder writes a flat output directory (angular.json's
# outputPath is "dist", index.html sits directly inside it). The newer
# `@angular/build:application` builder would write dist/<project>/browser
# instead - if this project ever migrates to it, this line and nginx's root
# need to move with it.
COPY --from=builder /app/dist /usr/share/nginx/html

EXPOSE 80
