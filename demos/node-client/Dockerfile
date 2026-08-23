# Build from the repository root so the contract protos are in context:
#   docker build -f demos/node-client/Dockerfile -t grpc-lol-html-demo .
#
# lib/lolhtml.js resolves the service contract at ../../../proto relative to
# itself, so with the app at /app the protos belong at /proto.
#
# Both stages are Docker Hardened Images. The runtime has no shell, no
# package manager and no wget: every build step happens in the -dev stage
# and the healthcheck runs through node itself.
FROM dhi.io/node:24-alpine-dev AS build
WORKDIR /app
COPY demos/node-client/package.json demos/node-client/package-lock.json ./
RUN npm ci --omit=dev
COPY demos/node-client/server.js ./
COPY demos/node-client/lib ./lib
COPY demos/node-client/public ./public

FROM dhi.io/node:24-alpine
WORKDIR /app
COPY --from=build /app /app
# server.js resolves the samples at ../sample-data relative to /app. They are
# committed under demos/sample-data, so bake them in at /sample-data.
COPY demos/sample-data /sample-data
COPY proto /proto
ENV PORT=8080
EXPOSE 8080
# The hardened runtime already runs as uid 1000. No wget/curl exists, so the
# healthcheck asks node; the server answers 200 at UI_BASE.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
  CMD ["node", "-e", "fetch(`http://127.0.0.1:${process.env.PORT||8080}${process.env.UI_BASE||''}/`).then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"]
CMD ["node", "server.js"]
