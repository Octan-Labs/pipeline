import { Logger } from '@nestjs/common';

export enum ElapsedUnit {
  Hour,
  Minute,
  Second,
  Milisec,
}

export class Timer {
  private logger?: Logger;
  private _start: Date;
  private _end: Date;

  constructor(logger?: Logger) {
    this.logger = logger;
    this._start = new Date();
  }

  start() {
    this._start = new Date();
  }

  /**
   * @returns number of milisecs elapsed
   */
  elapsedMilisecs(): number {
    this._end = new Date();
    return this._end.valueOf() - this._start.valueOf();
  }

  /**
   * @returns number of seconds elapsed
   */
  elapsedSeconds(): number {
    this._end = new Date();
    return (this._end.valueOf() - this._start.valueOf()) / 1000;
  }

  /**
   * @returns number of minutes elapsed
   */
  elapsedMinutes(): number {
    this._end = new Date();
    return (this._end.valueOf() - this._start.valueOf()) / 1000 / 60;
  }

  /**
   * @returns number of hours elapsed
   */
  elapsedHours(): number {
    this._end = new Date();
    return (this._end.valueOf() - this._start.valueOf()) / 1000 / 60 / 60;
  }

  logElapsed(unit?: ElapsedUnit) {
    let elapsedTime: [number, string];
    switch (unit) {
      case ElapsedUnit.Hour:
        elapsedTime = [this.elapsedHours(), 'hours'];
        break;
      case ElapsedUnit.Minute:
        elapsedTime = [this.elapsedMinutes(), 'minutes'];
        break;
      case ElapsedUnit.Second:
        elapsedTime = [this.elapsedSeconds(), 'seconds'];
        break;
      case ElapsedUnit.Milisec:
      default:
        elapsedTime = [this.elapsedMilisecs(), 'milisecs'];
        break;
    }
    const text = `Time elapsed: ${elapsedTime[0]} ${elapsedTime[1]}`;

    this._end = new Date();
    if (this.logger) {
      this.logger.log(text);
    } else {
      console.log(text);
    }
  }
}
