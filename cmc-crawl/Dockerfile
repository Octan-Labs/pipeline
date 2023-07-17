FROM node:20-alpine

WORKDIR .

COPY package*.json ./

RUN npm install

COPY . .

RUN mkdir cmc_historical
CMD ["node", "get-cmc-historical-price-top-1000.js"]