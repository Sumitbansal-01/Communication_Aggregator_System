# pinned base
FROM node:20-alpine3.19

WORKDIR /app

# use lockfile if available
COPY package*.json ./

# production-only deps
ENV NODE_ENV=production
RUN npm install --production --no-audit --no-fund

# copy app
COPY . .

# non-root user
USER node

EXPOSE 3000
CMD ["npm","start"]

