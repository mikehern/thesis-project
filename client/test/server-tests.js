const expect = require('chai').expect;
const request = require('request');
const supertest = require('supertest');
var bodyParser = require('body-parser');
var express = require('express');

// var app = express()
// app.use(bodyParser.json())
// app.use(bodyParser.urlencoded({ extended: false }))

const server = supertest.agent('http://localhost:1337');

describe('User arrives on homepage.', () => {
  describe(`Client connects to /`, () => {
    it('Responds with a 200', (done) => {
      server
        .get('/')
        .expect(200, done);
    });
  });
});

describe('User clicks on an Experiences item.', () => {
  describe('Client POSTS to our server.', () => {
    xit('Connects to the server', (done) => {
      server
        .post('/events')
        .set("Connection", "keep alive")
        .set("Content-Type", "application/json")
        .type("form")
        .send({event_type: "CLICK",
          experience_id: "999999999999",
          experiment_type: "TEST",
          user_id: "000000000000"
        })
        .expect(200, done);
    });
  });

  describe('DB includes written event', () => {
    xit('Row contains original payload', () => {

    });
    xit('Row includes timestamp', () => {

    });
  });

  describe('Aggregator receives event', () => {
    xit(`Aggregator's queue includes the original payload`, () => {

    });
  });
});