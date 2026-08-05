FROM node:18.20.8-bullseye-slim@sha256:d69fb189fa7765636655db043a6a9e6be5ddf94bd1a8dc33fd0bcf4664606a9e

RUN mkdir -p /home/app

WORKDIR /home/app/

COPY ./ ./

RUN npm ci
LABEL org.opencontainers.image.source https://github.com/sourcifyeth/verifier-alliance
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

CMD ["node", "pullFromVera.mjs" ]
