# pinned base
FROM node:20-alpine3.19

WORKDIR /app

# use lockfile if available
COPY package*.json ./

# production-only deps
ENV NODE_ENV=production
RUN npm ci --omit=dev --no-audit --no-fund

# copy app
COPY . .

# non-root user
USER node

EXPOSE 3001
CMD ["npm","start"]

