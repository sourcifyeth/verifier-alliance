# Needs to be run from the project root context
FROM node:18.18.2-bullseye as builder
RUN mkdir -p /home/app
WORKDIR /home/app

COPY  . .

RUN npm ci --workspace=sourcify-verifier-alliance --include-workspace-root

######################
## Production image ##
######################
FROM node:18.18.2-bullseye-slim as production

RUN mkdir -p /home/app/services/verifier-alliance

WORKDIR /home/app/
COPY package.json ./package.json
COPY package-lock.json ./package-lock.json
COPY lerna.json ./lerna.json
COPY nx.json ./nx.json

COPY --from=builder /home/app/packages/ ./packages/
COPY --from=builder /home/app/services/verifier-alliance/ ./services/verifier-alliance/

RUN npm ci --workspace=sourcify-verifier-alliance --include-workspace-root --omit=dev
LABEL org.opencontainers.image.source https://github.com/ethereum/sourcify
LABEL org.opencontainers.image.licenses MIT

ARG VERA_HOST
ARG VERA_DB
ARG VERA_USER
ARG VERA_PASSWORD
ARG VERA_PORT
ARG SOURCIFY_SERVER_HOST

# Set default value for ARG
ARG NODE_ENV=production

# Set environment variable
ENV NODE_ENV=${NODE_ENV}

WORKDIR /home/app/services/verifier-alliance

CMD ["node", "pullFromVera.mjs" ]
