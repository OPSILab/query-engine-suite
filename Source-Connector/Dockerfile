FROM node:24-alpine
WORKDIR /app
COPY . . 
#COPY ../utils ../utils
RUN npm ci --omit=dev
COPY ./token.template.js ./token.js
CMD ["node", "index"]