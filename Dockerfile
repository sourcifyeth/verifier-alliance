FROM node:18.18.2-bullseye-slim

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
