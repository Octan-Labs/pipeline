FROM node:18-alpine

WORKDIR /cmc-crawl

COPY package*.json /cmc-crawl

RUN npm install

COPY . /cmc-crawl

CMD ["node", "start"]