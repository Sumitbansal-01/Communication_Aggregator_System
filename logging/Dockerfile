# pinned base
FROM node:20-alpine3.19

WORKDIR /app

# copy lockfile + manifest for deterministic install
COPY package*.json ./

# production-only deps
ENV NODE_ENV=production
RUN npm install --production --no-audit --no-fund

# copy app code
COPY . .

# ensure node user owns files (prevents permission issues)
RUN chown -R node:node /app

# run as non-root
USER node

EXPOSE 3002
CMD ["npm","start"]
