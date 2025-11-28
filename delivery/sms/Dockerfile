FROM node:20-alpine3.19
WORKDIR /app
COPY package*.json ./
RUN npm install --production --no-audit --no-fund
COPY . .
# ensure non-root can read files (image includes 'node' user)
RUN chown -R node:node /app
USER node
EXPOSE 3000
CMD ["node","index.js"]
