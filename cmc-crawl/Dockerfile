FROM node:20-alpine

WORKDIR .

COPY package*.json ./

RUN npm install

USER ADMIN

COPY . .

CMD ["mkdir", "cmc_historical"]
