FROM node:20-alpine3.19
WORKDIR /app
COPY package*.json ./
RUN npm install --production --no-audit --no-fund
COPY . .
RUN chown -R node:node /app
USER node
CMD ["node","index.js"]
