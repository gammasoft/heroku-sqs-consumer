var SqsConsumer = require('sqs-consumer');
var moment = require('moment');
var request = require('request');
var _ = require('underscore');

function HerokuSqsConsumer (options) {
    var app = SqsConsumer.create({
        sqs: options.sqs,
        queueUrl: options.queueUrl,
        handleMessage: this._handleMessage(options.handleMessage)
    });

    app.on('error', function (err) {
        console.log(err);
        throw err;
    });

    this._app = app;
    this._lastJobDone = null;
    this._isHandlingMessage = false;
    this._options = _.extend({
        ttl: '5 minutes',
        interval: 1000
    }, options);

    return this;
}

HerokuSqsConsumer.prototype.start = function () {
    this._app.start();
    this._lastJobDone = new moment();

    var _this = this;

    var intervalId = setInterval(function () {
        if(_this._isHandlingMessage) {
            return;
        }

        if(_this._isExpired()) {
            _this.stop({
                shutdown: true
            });

            clearInterval(intervalId);
        }
    }, this._options.interval);
}

HerokuSqsConsumer.prototype._expiryDate = function () {
    var ttl = this._options.ttl.split(' '),
        timeValue = parseInt(ttl[0], 10),
        timeComponent = ttl[1],
        expiryDate = this._lastJobDone;

    return new moment(expiryDate).add(timeValue, timeComponent);
}

HerokuSqsConsumer.prototype._isExpired = function () {
    var now = new moment(),
        expiryDate = this._expiryDate();

    return now.isAfter(expiryDate);
}

HerokuSqsConsumer.prototype.stop = function (options) {
    this._app.stop();

    if(options && options.shutdown && this._options.heroku) {
        stopDynos(this._options.heroku);
    }
}

HerokuSqsConsumer.prototype._handleMessage = function (handleMessage) {
    var _this = this;

    return function (message, done) {
        function jobDone(result, next) {
            _this._isHandlingMessage = false;
            _this._lastJobDone = new moment();

            done(result);

            if(next) {
                // TODO: Add message to next queue
                throw new Error('next is not implemented yet');
            }
        }

        _this._isHandlingMessage = true;
        _this._options.handleMessage(message, jobDone, _.partial(jobDone, _, true));
    }
}

function stopDynos(options) {
    request({
        method: 'PATCH',
        url: 'https://api.heroku.com/apps/' + options.app + '/formation/' + options.processType,
        headers: {
            Accept: 'application/vnd.heroku+json; version=3',
            Authorization: 'Bearer ' + options.token
        },
        body: JSON.stringify({
            quantity: 0
        })
    }, function(err, res, body) {
        if(err) {
            throw err; // TODO: Não lançar
        }

        if(res.statusCode !== 200) {
            console.log(body);
            console.log('Problem shutting down Heroku, status code was not OK: ' + res.statusCode);
        }
    });
}

module.exports = HerokuSqsConsumer;