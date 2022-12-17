/* eslint-disable no-undef */
import { MongooseUtilities } from "@threeceelabs/mongoose-utilities";
import Stately from "stately.js";
import { ThreeceeUtilities } from "@threeceelabs/threeceeutilities";
import _ from "lodash";
import async from "async";
import chalk from "chalk";
import cla from "command-line-args";
import debug from "debug";
import deepcopy from "deep-copy";
import dotenv from "dotenv";
import merge from "deepmerge";
import moment from "moment";
import os from "os";
import path from "path";
import pick from "object.pick";
import util from "util";

let hostname = os.hostname();
hostname = hostname.replace(/\.example\.com/g, "");
hostname = hostname.replace(/\.local/g, "");
hostname = hostname.replace(/\.home/g, "");
hostname = hostname.replace(/\.at\.net/g, "");
hostname = hostname.replace(/\.fios-router\.home/g, "");
hostname = hostname.replace(/word0-instance-1/g, "google");
hostname = hostname.replace(/word-1/g, "google");
hostname = hostname.replace(/word/g, "google");

const envConfig = dotenv.config({ path: process.env.WORD_ENV_VARS_FILE });

if (envConfig.error) {
  throw envConfig.error;
}
const MODULE_NAME = "pus";
const PF = "PUS";

console.log(`${PF} | +++ ENV CONFIG LOADED`);
const DEFAULT_MAX_LAST_SEEN_DAYS = 60;
const TEST_MODE = false; // applies only to parent
const QUIT_ON_COMPLETE = false;
const ONE_SECOND = 1000.0;
const ONE_MINUTE = ONE_SECOND * 60.0;
const compactDateTimeFormat = "YYYYMMDD_HHmmss";
const DEFAULT_TOTAL_USERS_TO_FETCH = "ALL";
const DEFAULT_TEST_TOTAL_USERS_TO_FETCH = 10;
const DEFAULT_RESTART_DELAY = ONE_MINUTE;
const FETCH_USER_INTERVAL = 5;
const TEST_FETCH_USER_INTERVAL = 10;
const KEEPALIVE_INTERVAL = ONE_MINUTE;
const QUIT_WAIT_INTERVAL = ONE_SECOND;
const STATS_UPDATE_INTERVAL = 5 * ONE_MINUTE;
const SAVE_CACHE_DEFAULT_TTL = 60;
const SAVE_FILE_QUEUE_INTERVAL = ONE_SECOND;
const FSM_TICK_INTERVAL = ONE_SECOND;

let stdin;
let abortCursor = false;

const chalkGreen = chalk.green;
const chalkBlueBold = chalk.blue.bold;
const chalkError = chalk.bold.red;
const chalkAlert = chalk.red;
const chalkLog = chalk.gray;
const chalkInfo = chalk.black;

const MODULE_ID = PF + "_" + hostname;

const mguAppName = "MGU_" + MODULE_ID;
const mgUtils = new MongooseUtilities(mguAppName);

mgUtils.on("error", async (err) => {
  console.error(
    `${PF} | *** MONGOOSE UTILS ERROR | ${mguAppName} | ERR: ${err}`
  );
});

mgUtils.on("connect", async () => {
  console.log(`${PF} | +++ MONGOOSE UTILS CONNECT: ${mguAppName}`);
  tcUtils.emitter.emit("MONGO_DB_CONNECTED");
});

mgUtils.on("ready", async () => {
  console.log(`${PF} | +++ MONGOOSE UTILS READY: ${mguAppName}`);
});

const tcuAppName = PF + "_TCU";
const tcUtils = new ThreeceeUtilities(tcuAppName);

tcUtils.on("ready", async () => {
  console.log(`${PF} | +++ THREECEE UTILS READY: ${tcuAppName}`);
});

const jsonPrint = tcUtils.jsonPrint;
const msToTime = tcUtils.msToTime;
const getTimeStamp = tcUtils.getTimeStamp;
const formatBoolean = tcUtils.formatBoolean;
const formatCategory = tcUtils.formatCategory;

const startTimeMoment = moment();

function getElapsedTimeStamp() {
  statsObj.elapsedMS = moment().valueOf() - startTimeMoment.valueOf();
  return msToTime(statsObj.elapsedMS);
}

//=========================================================================
// PROCESS EVENT HANDLERS
//=========================================================================

process.title = MODULE_ID.toLowerCase() + "_node_" + process.pid;

process.on("exit", function () {
  quit({ cause: "PARENT EXIT" });
});

process.on("close", function () {
  quit({ cause: "PARENT CLOSE" });
});

process.on("disconnect", function () {
  process.exit(1);
});

process.on("SIGHUP", function () {
  quit({ cause: "PARENT SIGHUP" });
});

process.on("SIGINT", function () {
  quit({ cause: "PARENT SIGINT" });
});

process.on("unhandledRejection", function () {
  quit("unhandledRejection");
  process.exit(1);
});

const saveFileQueue = [];

//=========================================================================
// CONFIGURATION
//=========================================================================

let configuration = {};

let defaultConfiguration = {}; // general configuration for PUS
let hostConfiguration = {}; // host-specific configuration for PUS
configuration.maxLastSeenDays = DEFAULT_MAX_LAST_SEEN_DAYS;
configuration.restartDelay = DEFAULT_RESTART_DELAY;
configuration.testMode = TEST_MODE;
configuration.statsUpdateIntervalTime = STATS_UPDATE_INTERVAL;
configuration.fsmTickInterval = FSM_TICK_INTERVAL;
configuration.totalUsersToFecth = DEFAULT_TOTAL_USERS_TO_FETCH;
configuration.testTotalUsersToFecth = DEFAULT_TEST_TOTAL_USERS_TO_FETCH;
configuration.fetchUserInterval = TEST_MODE
  ? TEST_FETCH_USER_INTERVAL
  : FETCH_USER_INTERVAL;

configuration.slackChannel = {};
configuration.keepaliveInterval = KEEPALIVE_INTERVAL;
configuration.quitOnComplete = QUIT_ON_COMPLETE;

const statsObj = {};
let statsObjSmall = {};

statsObj.botLimit = false;
statsObj.pid = process.pid;
statsObj.runId = MODULE_ID.toLowerCase() + "_" + getTimeStamp();

statsObj.hostname = hostname;
statsObj.startTime = getTimeStamp();
statsObj.elapsedMS = 0;
statsObj.elapsed = getElapsedTimeStamp();

statsObj.queues = {};
statsObj.queues.saveFileQueue = {};
statsObj.queues.saveFileQueue.busy = false;
statsObj.queues.saveFileQueue.size = 0;

statsObj.queues.fetchUser = {};
statsObj.queues.fetchUser.busy = false;
statsObj.queues.fetchUser.size = 0;

statsObj.errors = {};
statsObj.errors.fetchUsers = 0;

statsObj.status = "START";

statsObj.users = {};
statsObj.users.total = 0;
statsObj.users.fetched = 0;
statsObj.users.skipped = 0;
statsObj.users.remaining = 0;
statsObj.users.deleted = 0;
statsObj.users.fetchErrors = 0;
statsObj.users.percentFetched = 0;
statsObj.users.fetchedRateMS = 0;
statsObj.remainingTimeMs = 0;

// ==================================================================
// DROPBOX
// ==================================================================
let DROPBOX_ROOT_FOLDER;

if (hostname === "google") {
  DROPBOX_ROOT_FOLDER = "/home/tc/Dropbox/Apps/wordAssociation";
} else {
  DROPBOX_ROOT_FOLDER = "/Users/tc/Dropbox/Apps/wordAssociation";
}

configuration.DROPBOX = {};

configuration.DROPBOX.DROPBOX_CONFIG_FILE =
  process.env.DROPBOX_CONFIG_FILE || `${PF.toLowerCase()}Config.json`;
configuration.DROPBOX.DROPBOX_STATS_FILE =
  process.env.DROPBOX_STATS_FILE || `${PF.toLowerCase()}Stats.json`;

const configDefaultFolder = path.join(
  DROPBOX_ROOT_FOLDER,
  "config/utility/default"
);
const configHostFolder = path.join(
  DROPBOX_ROOT_FOLDER,
  "config/utility",
  hostname
);

const configDefaultFile =
  "default_" + configuration.DROPBOX.DROPBOX_CONFIG_FILE;
const configHostFile =
  hostname + "_" + configuration.DROPBOX.DROPBOX_CONFIG_FILE;

const statsFolder = path.join(DROPBOX_ROOT_FOLDER, hostname, "stats");
const statsFile = configuration.DROPBOX.DROPBOX_STATS_FILE;

const lastUserFetchedFile =
  hostname + "_" + MODULE_NAME + "LastUserFetched.json";
let lastUserFetched = false;

let fetchUserReady = true;

const statsPickArray = [
  "pid",
  "startTime",
  "elapsed",
  "error",
  "elapsedMS",
  "status",
  "queues",
];

async function updateStats() {
  statsObj.elapsed = getElapsedTimeStamp();
  statsObj.timeStamp = getTimeStamp();
  statsObj.elapsedMS = moment().valueOf() - startTimeMoment.valueOf();
  statsObj.users.remaining = statsObj.users.total - statsObj.users.fetched;
  statsObj.users.percentFetched =
    100 * (statsObj.users.fetched / statsObj.users.total);

  if (statsObj.users.fetched) {
    statsObj.users.fetchedRateMS = statsObj.elapsedMS / statsObj.users.fetched; // ms/fetched
  }

  statsObj.remainingTimeMs =
    statsObj.users.fetchedRateMS * statsObj.users.remaining;
  return;
}

async function showStats(options) {
  await updateStats();

  statsObjSmall = pick(statsObj, statsPickArray);

  if (options) {
    console.log(PF + " | STATS\n" + jsonPrint(statsObjSmall));
    return;
  } else {
    console.log(
      chalkLog(
        `${PF} | STATUS` +
          ` | @${configuration.threeceeUser}` +
          ` | FSM: ${fsm.getMachineState()}` +
          ` | START: ${statsObj.startTime}` +
          ` | NOW: ${getTimeStamp()}` +
          ` | ELAPSED: ${statsObj.elapsed}` +
          ` | ${statsObj.users.deleted} USERS DELETED` +
          ` | LAST FETCHED: ${lastUserFetched}`
      )
    );
    return;
  }
}

const help = { name: "help", alias: "h", type: Boolean };
const enableStdin = {
  name: "enableStdin",
  alias: "S",
  type: Boolean,
  defaultValue: true,
};
const quitOnComplete = { name: "quitOnComplete", alias: "q", type: Boolean };
const quitOnError = {
  name: "quitOnError",
  alias: "Q",
  type: Boolean,
  defaultValue: true,
};
const verbose = { name: "verbose", alias: "v", type: Boolean };
const testMode = { name: "testMode", alias: "X", type: Boolean };
const optionDefinitions = [
  enableStdin,
  quitOnComplete,
  quitOnError,
  verbose,
  testMode,
  help,
];

const commandLineConfig = cla(optionDefinitions);

console.log(
  chalkInfo(PF + " | COMMAND LINE CONFIG\n" + jsonPrint(commandLineConfig))
);

if (Object.keys(commandLineConfig).includes("help")) {
  console.log(PF + " |optionDefinitions\n" + jsonPrint(optionDefinitions));
  quit("help");
}

statsObj.commandLineConfig = commandLineConfig;

function loadCommandLineArgs() {
  return new Promise(function (resolve) {
    statsObj.status = "LOAD COMMAND LINE ARGS";

    const commandLineConfigKeys = Object.keys(commandLineConfig);

    async.each(
      commandLineConfigKeys,
      function (arg, cb) {
        configuration[arg] = commandLineConfig[arg];
        console.log(
          PF + " | --> COMMAND LINE CONFIG | " + arg + ": " + configuration[arg]
        );

        cb();
      },
      function () {
        statsObj.commandLineArgsLoaded = true;
        resolve();
      }
    );
  });
}

function toggleVerbose() {
  configuration.verbose = !configuration.verbose;

  console.log(chalkLog(PF + " | VERBOSE: " + configuration.verbose));
}

function initStdIn() {
  console.log(PF + " | STDIN ENABLED");
  stdin = process.stdin;
  if (stdin.setRawMode !== undefined) {
    stdin.setRawMode(true);
  }
  stdin.resume();
  stdin.setEncoding("utf8");
  stdin.on("data", async function (key) {
    switch (key) {
      case "a":
        abortCursor = true;
        console.log(chalkLog(PF + " | STDIN | ABORT: " + abortCursor));
        break;

      case "K":
        quit({ force: true });
        break;

      case "q":
        quit({ source: "STDIN" });
        break;
      case "Q":
        process.exit();
        break;

      case "S":
      case "s":
        try {
          await showStats(key === "S");
        } catch (err) {
          console.log(chalkError(PF + " | *** SHOW STATS ERROR: " + err));
        }
        break;

      case "V":
        toggleVerbose();
        break;

      default:
        console.log(
          chalkInfo(
            "\nTFF | " +
              "q/Q: quit" +
              "\nTFF | " +
              "s: showStats" +
              "\nTFF | " +
              "S: showStats verbose" +
              "\nTFF | " +
              "V: toggle verbose"
          )
        );
    }
  });
}

function initStatsUpdate() {
  return new Promise(function (resolve) {
    console.log(
      chalkLog(
        PF +
          " | INIT STATS UPDATE INTERVAL" +
          " | " +
          msToTime(configuration.statsUpdateIntervalTime)
      )
    );

    statsObj.elapsed = getElapsedTimeStamp();
    statsObj.timeStamp = getTimeStamp();

    tcUtils.saveFile({ folder: statsFolder, file: statsFile, obj: statsObj });

    clearInterval(statsUpdateInterval);

    statsUpdateInterval = setInterval(async function () {
      statsObj.elapsed = getElapsedTimeStamp();
      statsObj.timeStamp = getTimeStamp();

      saveFileQueue.push({
        folder: statsFolder,
        file: statsFile,
        obj: statsObj,
      });
      statsObj.queues.saveFileQueue.size = saveFileQueue.length;

      try {
        await showStats();
      } catch (err) {
        console.log(chalkError(PF + " | *** SHOW STATS ERROR: " + err));
      }
    }, configuration.statsUpdateIntervalTime);

    resolve();
  });
}

async function initConfig(cnf) {
  statsObj.status = "INIT CONFIG";

  if (debug.enabled) {
    console.log(
      "\nTFF | %%%%%%%%%%%%%%\nTFF |  DEBUG ENABLED \nTFF | %%%%%%%%%%%%%%\n"
    );
  }

  cnf.processName = process.env.PROCESS_NAME || MODULE_ID;
  cnf.testMode = process.env.TEST_MODE === "true" ? true : cnf.testMode;
  cnf.quitOnError = process.env.QUIT_ON_ERROR || false;

  if (process.env.QUIT_ON_COMPLETE === "false") {
    cnf.quitOnComplete = false;
  } else if (
    process.env.QUIT_ON_COMPLETE === true ||
    process.env.QUIT_ON_COMPLETE === "true"
  ) {
    cnf.quitOnComplete = true;
  }

  try {
    await loadAllConfigFiles();
    await loadCommandLineArgs();

    const configArgs = Object.keys(configuration);

    configArgs.forEach(function (arg) {
      if (_.isObject(configuration[arg])) {
        console.log(
          PF +
            " | _FINAL CONFIG | " +
            arg +
            "\n" +
            jsonPrint(configuration[arg])
        );
      } else {
        console.log(
          PF + " | _FINAL CONFIG | " + arg + ": " + configuration[arg]
        );
      }
    });

    statsObj.commandLineArgsLoaded = true;

    if (configuration.enableStdin) {
      initStdIn();
    }

    await initStatsUpdate();

    if (configuration.testMode) {
      console.log(`${PF} | *** TEST MODE ***`);
    }
    return configuration;
  } catch (err) {
    console.log(chalkError(PF + " | *** CONFIG LOAD ERROR: " + err));
    throw err;
  }
}

//=========================================================================
// STATS
//=========================================================================

async function loadConfigFile(params) {
  const fullPath = path.join(params.folder, params.file);

  try {
    if (configuration.offlineMode) {
      await loadCommandLineArgs();
      return;
    }

    const newConfiguration = {};
    newConfiguration.evolve = {};

    const loadedConfigObj = await tcUtils.loadFile({
      folder: params.folder,
      file: params.file,
      noErrorNotFound: params.noErrorNotFound,
    });

    if (loadedConfigObj === undefined) {
      if (params.noErrorNotFound) {
        console.log(
          chalkAlert(
            PF +
              " | ... SKIP LOAD CONFIG FILE: " +
              params.folder +
              "/" +
              params.file
          )
        );
        return newConfiguration;
      } else {
        console.log(
          chalkError(PF + " | *** CONFIG LOAD FILE ERROR | JSON UNDEFINED ??? ")
        );
        throw new Error("JSON UNDEFINED");
      }
    }

    if (loadedConfigObj instanceof Error) {
      console.log(
        chalkError(PF + " | *** CONFIG LOAD FILE ERROR: " + loadedConfigObj)
      );
    }

    console.log(
      chalkInfo(
        PF +
          " | LOADED CONFIG FILE: " +
          params.file +
          "\n" +
          jsonPrint(loadedConfigObj)
      )
    );

    if (loadedConfigObj.PUS_TEST_MODE !== undefined) {
      console.log(
        "PUS | LOADED PUS_TEST_MODE: " + loadedConfigObj.PUS_TEST_MODE
      );
      if (
        loadedConfigObj.PUS_TEST_MODE === true ||
        loadedConfigObj.PUS_TEST_MODE === "true"
      ) {
        newConfiguration.testMode = true;
      }
      if (
        loadedConfigObj.PUS_TEST_MODE === false ||
        loadedConfigObj.PUS_TEST_MODE === "false"
      ) {
        newConfiguration.testMode = false;
      }
    }

    if (loadedConfigObj.PUS_FETCH_NEXT_CURSOR !== undefined) {
      console.log(
        "PUS | LOADED PUS_FETCH_NEXT_CURSOR: " +
          loadedConfigObj.PUS_FETCH_NEXT_CURSOR
      );
      if (
        loadedConfigObj.PUS_FETCH_NEXT_CURSOR === true ||
        loadedConfigObj.PUS_FETCH_NEXT_CURSOR === "true"
      ) {
        newConfiguration.fetchNextCursor = true;
      }
      if (
        loadedConfigObj.PUS_FETCH_NEXT_CURSOR === false ||
        loadedConfigObj.PUS_FETCH_NEXT_CURSOR === "false"
      ) {
        newConfiguration.fetchNextCursor = false;
      }
    }

    if (loadedConfigObj.PUS_QUIT_ON_COMPLETE !== undefined) {
      console.log(
        "PUS | LOADED PUS_QUIT_ON_COMPLETE: " +
          loadedConfigObj.PUS_QUIT_ON_COMPLETE
      );
      if (
        loadedConfigObj.PUS_QUIT_ON_COMPLETE === true ||
        loadedConfigObj.PUS_QUIT_ON_COMPLETE === "true"
      ) {
        newConfiguration.quitOnComplete = true;
      }
      if (
        loadedConfigObj.PUS_QUIT_ON_COMPLETE === false ||
        loadedConfigObj.PUS_QUIT_ON_COMPLETE === "false"
      ) {
        newConfiguration.quitOnComplete = false;
      }
    }

    if (loadedConfigObj.PUS_RESTART_DELAY_MINUTES !== undefined) {
      console.log(
        "PUS | LOADED PUS_RESTART_DELAY_MINUTES: " +
          loadedConfigObj.PUS_RESTART_DELAY_MINUTES
      );
      newConfiguration.restartDelay =
        loadedConfigObj.PUS_RESTART_DELAY_MINUTES * ONE_MINUTE;
    }

    if (loadedConfigObj.PUS_VERBOSE !== undefined) {
      console.log("PUS | LOADED PUS_VERBOSE: " + loadedConfigObj.PUS_VERBOSE);
      if (
        loadedConfigObj.PUS_VERBOSE === true ||
        loadedConfigObj.PUS_VERBOSE === "true"
      ) {
        newConfiguration.verbose = true;
      }
      if (
        loadedConfigObj.PUS_VERBOSE === false ||
        loadedConfigObj.PUS_VERBOSE === "false"
      ) {
        newConfiguration.verbose = false;
      }
    }

    if (loadedConfigObj.PUS_FETCH_USER_INTERVAL !== undefined) {
      console.log(
        "PUS | LOADED PUS_FETCH_USER_INTERVAL: " +
          loadedConfigObj.PUS_FETCH_USER_INTERVAL
      );
      newConfiguration.fetchUserInterval =
        loadedConfigObj.PUS_FETCH_USER_INTERVAL;
    }

    if (loadedConfigObj.PUS_ENABLE_STDIN !== undefined) {
      console.log(
        "PUS | LOADED PUS_ENABLE_STDIN: " + loadedConfigObj.PUS_ENABLE_STDIN
      );
      newConfiguration.enableStdin = loadedConfigObj.PUS_ENABLE_STDIN;
    }

    if (loadedConfigObj.PUS_KEEPALIVE_INTERVAL !== undefined) {
      console.log(
        "PUS | LOADED PUS_KEEPALIVE_INTERVAL: " +
          loadedConfigObj.PUS_KEEPALIVE_INTERVAL
      );
      newConfiguration.keepaliveInterval =
        loadedConfigObj.PUS_KEEPALIVE_INTERVAL;
    }

    return newConfiguration;
  } catch (err) {
    console.error(
      chalkError(
        PF + " | ERROR LOAD CONFIG: " + fullPath + "\n" + jsonPrint(err)
      )
    );
    throw err;
  }
}

async function loadAllConfigFiles() {
  statsObj.status = "LOAD CONFIG";

  const defaultConfig = await loadConfigFile({
    folder: configDefaultFolder,
    file: configDefaultFile,
    noErrorNotFound: true,
  });

  if (defaultConfig) {
    defaultConfiguration = defaultConfig;
    console.log(
      chalkInfo(
        PF +
          " | <<< LOADED DEFAULT CONFIG " +
          configDefaultFolder +
          "/" +
          configDefaultFile
      )
    );
  }

  const hostConfig = await loadConfigFile({
    folder: configHostFolder,
    file: configHostFile,
    noErrorNotFound: true,
  });

  if (hostConfig) {
    hostConfiguration = hostConfig;
    console.log(
      chalkInfo(
        PF +
          " | <<< LOADED HOST CONFIG " +
          configHostFolder +
          "/" +
          configHostFile
      )
    );
  }

  const defaultAndHostConfig = merge(defaultConfiguration, hostConfiguration); // host settings override defaults
  const tempConfig = merge(configuration, defaultAndHostConfig); // any new settings override existing config

  configuration = deepcopy(tempConfig);

  configuration.twitterUsers = _.uniq(configuration.twitterUsers);

  return;
}

//=========================================================================
// FILE SAVE
//=========================================================================
let saveFileQueueInterval;
let statsUpdateInterval;

configuration.saveFileQueueInterval = SAVE_FILE_QUEUE_INTERVAL;

let saveCacheTtl = process.env.SAVE_CACHE_DEFAULT_TTL;

if (saveCacheTtl === undefined) {
  saveCacheTtl = SAVE_CACHE_DEFAULT_TTL;
}

console.log(PF + " | SAVE CACHE TTL: " + saveCacheTtl + " SECONDS");

let saveCacheCheckPeriod = process.env.SAVE_CACHE_CHECK_PERIOD;

if (saveCacheCheckPeriod === undefined) {
  saveCacheCheckPeriod = 10;
}

console.log(
  PF + " | SAVE CACHE CHECK PERIOD: " + saveCacheCheckPeriod + " SECONDS"
);

function initSaveFileQueue(cnf) {
  console.log(
    chalkLog(
      PF + " | INIT SAVE FILE INTERVAL | " + msToTime(cnf.saveFileQueueInterval)
    )
  );

  clearInterval(saveFileQueueInterval);

  saveFileQueueInterval = setInterval(async function () {
    if (!statsObj.queues.saveFileQueue.busy && saveFileQueue.length > 0) {
      statsObj.queues.saveFileQueue.busy = true;

      const saveFileObj = saveFileQueue.shift();
      // saveFileObj.verbose = true;

      statsObj.queues.saveFileQueue.size = saveFileQueue.length;

      try {
        await tcUtils.saveFile(saveFileObj);
        debug(
          chalkLog(
            PF +
              " | SAVED FILE" +
              " [Q: " +
              saveFileQueue.length +
              "] " +
              saveFileObj.folder +
              "/" +
              saveFileObj.file
          )
        );
        statsObj.queues.saveFileQueue.busy = false;
      } catch (err) {
        console.log(
          chalkError(
            PF +
              " | *** SAVE FILE ERROR ... RETRY" +
              " | ERROR: " +
              err +
              " | " +
              saveFileObj.folder +
              "/" +
              saveFileObj.file
          )
        );
        saveFileQueue.push(saveFileObj);
        statsObj.queues.saveFileQueue.size = saveFileQueue.length;
        statsObj.queues.saveFileQueue.busy = false;
      }
    }
  }, cnf.saveFileQueueInterval);
  return;
}

//=========================================================================
// INTERVALS
//=========================================================================
const intervalsSet = new Set();

function clearAllIntervals() {
  return new Promise(function (resolve, reject) {
    try {
      [...intervalsSet].forEach(function (intervalHandle) {
        clearInterval(intervalHandle);
      });
      resolve();
    } catch (err) {
      reject(err);
    }
  });
}

//=========================================================================
// QUIT + EXIT
//=========================================================================

function readyToQuit() {
  const flag = true; // replace with function returns true when ready to quit
  return flag;
}

async function quit(opts) {
  const options = opts || {};

  if (options) {
    console.log(PF + " | QUIT INFO\n" + jsonPrint(options));
  }

  statsObj.elapsed = getElapsedTimeStamp();
  statsObj.timeStamp = getTimeStamp();
  statsObj.status = "QUIT";

  fsm.fsm_exit();

  showStats(true);

  intervalsSet.add("quitWaitInterval");

  const quitWaitInterval = setInterval(async function () {
    if (readyToQuit()) {
      clearInterval(quitWaitInterval);
      await clearAllIntervals();
      if (global.dbConnection) {
        await global.dbConnection.close();
      }
      process.exit();
    }
  }, QUIT_WAIT_INTERVAL);
}

let fetchUserInterval;
intervalsSet.add("fetchUserInterval");

const handleMongooseEvent = async (eventObj) => {
  console.log({ eventObj });

  switch (eventObj.event) {
    case "end":
    case "close":
      console.log(`${PF} | CURSOR EVENT: ${eventObj.event.toUpperCase()}`);
      break;

    case "error":
      console.error(
        `${PF} | CURSOR ERROR | NAME: ${eventObj.err.name} | ${eventObj.err}`
      );
      if (eventObj.err.name.includes("MongoExpiredSessionError")) {
        cursor = await mgUtils.initCursor({
          query: eventObj.query,
          cursorLean: eventObj.cursorLean,
          cursorLimit: eventObj.cursorLimit,
        });
      }
      break;

    default:
      console.error(`*** UNKNOWN EVENT: ${eventObj.event}`);
  }
  return;
};

let cursor;

async function purgeUser(p) {
  const params = p || {};
  const user = params.user;
  // const testMode = params.testMode || configuration.testMode;
  // const minFollowers = params.minFollowers || configuration.minFollowers;
  const maxLastSeenDays =
    params.maxLastSeenDays || configuration.maxLastSeenDays;
  // const suspended = params.suspended || configuration.suspended;
  // const notFound = params.notFound || configuration.notFound;
  // const notAuthorized = params.notAuthorized || configuration.notAuthorized;

  const result = {};
  result.deleted = false;
  result.lastSeenDaysAgo = parseInt(
    moment.duration(moment().diff(moment(user.lastSeen))).as("days")
  );

  if (result.lastSeenDaysAgo > maxLastSeenDays) {
    result.deleteResult = await global.wordAssoDb.User.deleteOne({
      nodeId: user.nodeId,
    });
    result.deleted = true;

    debug(`${PF} | PURGE USER | @${user.screenName}`);
    statsObj.users.deleted += 1;
  }
  return result;
}

async function initfetchUsers(p) {
  console.log(chalkBlueBold(`${PF} | FETCH START`));
  const params = p || {};
  const testMode = params.testMode || configuration.testMode;
  const verbose = params.verbose || configuration.verbose;

  let query = {};

  if (params.query) {
    query = params.query;
  } else {
    query.categorized = false;
    query.categoryAuto = "none";
    query.followersCount = 0;
    query.friendsCount = 0;
    if (lastUserFetched) {
      query.nodeId = { $gt: lastUserFetched };
    }
  }

  if (testMode) {
    statsObj.users.total = configuration.testTotalUsersToFecth;
  } else if (configuration.totalUsersToFecth === "ALL") {
    console.log(`${PF} | COUNTING TOTAL USERS ...`);
    console.log({ query });
    statsObj.users.total = await global.wordAssoDb.User.countDocuments(query);
  }

  if (statsObj.users.total === 0) {
    console.log(
      chalk.yellow(
        `${PF} | TEST MODE: ${configuration.testMode} | NO USERS TO FETCH | TOTAL USER TO FETCH: ${statsObj.users.total} | SET lastUserFetched = 1000`
      )
    );
    lastUserFetched = "1000";
    query.nodeId = { $gt: lastUserFetched };
  }

  console.log(
    chalk.blue(
      `${PF} | TEST MODE: ${configuration.testMode} | TOTAL USER TO FETCH: ${statsObj.users.total}`
    )
  );

  clearInterval(fetchUserInterval);

  const interval = testMode
    ? TEST_FETCH_USER_INTERVAL
    : params.interval || configuration.fetchUserInterval;

  console.log(chalkInfo(`${PF} | FETCH USERS | INTERVAL: ${interval} ms`));
  console.log(chalkInfo(`${PF} | FETCH USERS | QUERY\n`, jsonPrint(query)));

  cursor = await mgUtils.initCursor({
    query: query,
    cursorLimit: configuration.testMode ? 10 : null,
    cursorLean: true,
    project: {
      nodeId: 1,
      screenName: 1,
      category: 1,
      categoryAuto: 1,
      createdAt: 1,
      lastSeen: 1,
      followersCount: 1,
      friendsCount: 1,
    },
  });

  cursor.on("error", async (err) =>
    handleMongooseEvent({ event: "error", err: err })
  );
  cursor.on("end", async () => handleMongooseEvent({ event: "end" }));
  cursor.on("close", async () => handleMongooseEvent({ event: "close" }));

  fetchUserReady = true;

  statsObj.users.fetched = 0;
  statsObj.users.skipped = 0;
  statsObj.queues.fetchUser.busy = !fetchUserReady;

  let previousUserId = lastUserFetched;

  fetchUserInterval = setInterval(async () => {
    if (fetchUserReady) {
      try {
        fetchUserReady = false;
        let user;
        try {
          user = await cursor.next();
          statsObj.users.fetched += 1;
        } catch (err) {
          if (err.name.includes("MongoExpiredSessionError")) {
            cursor = await mgUtils.initCursor({
              query,
              cursorLean: true,
            });
          }
        }
        if (!user) {
          clearInterval(fetchUserInterval);

          lastUserFetched = "RESTART";

          await tcUtils.saveFile({
            folder: configHostFolder,
            file: lastUserFetchedFile,
            obj: { nodeId: previousUserId },
          });

          fsm.fsm_fetchUserEnd();
          fetchUserReady = true;
          return;
        }

        const purgeUserResult = await purgeUser({ user });
        const chlk = purgeUserResult.deleted ? chalkInfo : chalkLog;

        statsObj.users.purgeRate =
          (100 * statsObj.users.deleted) / statsObj.users.fetched;

        statsObj.users.percentProcessed =
          (100 * statsObj.users.fetched) / statsObj.users.total;

        if (verbose || purgeUserResult.deleted)
          console.log(
            chlk(
              `${PF} | PURGE` +
                ` [ ${statsObj.users.deleted}` +
                ` / ${statsObj.users.fetched}` +
                ` / ${statsObj.users.total}` +
                ` / ${statsObj.users.purgeRate.toFixed(2)}% PURGE RATE` +
                ` - ${statsObj.users.percentProcessed.toFixed(2)}% PRCSD ]` +
                ` | LS: ${moment(user.lastSeen).format(
                  compactDateTimeFormat
                )}` +
                `, ${parseInt(purgeUserResult.lastSeenDaysAgo)} DAYS AGO` +
                ` - MAX: ${configuration.maxLastSeenDays}` +
                ` | DELETED: ${formatBoolean(purgeUserResult.deleted)}` +
                ` | CM: ${formatCategory(user.category)}` +
                ` | CA: ${formatCategory(user.categoryAuto)}` +
                ` | ${user.followersCount} FLWRs` +
                ` | ${user.friendsCount} FRNDs` +
                ` | NID: ${user.nodeId}` +
                ` | @${user.screenName}`
            )
          );
        previousUserId = user.nodeId;
        lastUserFetched = user.nodeId;

        await tcUtils.saveFile({
          folder: configHostFolder,
          file: lastUserFetchedFile,
          obj: { nodeId: user.nodeId },
        });

        fetchUserReady = true;
      } catch (err) {
        console.error(
          `${PF} | *** initfetchUsers ERROR | code: ${err.code} | message: ${err.message} ${err}`
        );
        if (err.message.includes("MongoExpiredSessionError")) {
          cursor = await mgUtils.initCursor({
            query: query,
            cursorLimit: configuration.testMode ? 10 : null,
            cursorLean: true,
          });
        }
        fetchUserReady = true;
      }
    }
  }, interval);

  return;
}

//=========================================================================
// FSM
//=========================================================================

let fsmTickInterval;
intervalsSet.add("fsmTickInterval");

statsObj.fsmState = "NEW";
statsObj.fsmPreviousState = "NEW";

function reporter(event, prevState, newState) {
  statsObj.fsmState = newState;
  statsObj.fsmPreviousState = prevState;
  statsObj.fsmPreviousPauseState =
    newState === "PAUSE_RATE_LIMIT" ? prevState : "FETCH";

  console.log(
    chalkLog(`${PF} | --------------------------------------------------------`)
  );
  console.log(
    chalkLog(
      `${PF} | << FSM >> | ${event} | ${statsObj.fsmPreviousState} -> ${newState}`
    )
  );
  console.log(
    chalkLog(`${PF} | --------------------------------------------------------`)
  );
}

const fsmStates = {
  RESET: {
    onEnter: function (event, prevState, newState) {
      if (event !== "fsm_tick") {
        reporter(event, prevState, newState);
        resetTwitterUserState();
      }
    },

    fsm_fetchUserEnd: "FETCH_END",
    fsm_exit: "EXIT",
    fsm_reset: "RESET",
    fsm_init: "INIT",
    fsm_error: "ERROR",
  },

  INIT: {
    onEnter: async function (event, prevState, newState) {
      if (event !== "fsm_tick") {
        reporter(event, prevState, newState);
        fsm.fsm_ready();
      }
    },

    fsm_tick: function () {},

    fsm_fetchUserEnd: "FETCH_END",
    fsm_ready: "READY",
    fsm_reset: "RESET",
    fsm_exit: "EXIT",
    fsm_error: "ERROR",
  },

  READY: {
    onEnter: async function (event, prevState, newState) {
      if (event !== "fsm_tick") {
        reporter(event, prevState, newState);
        fsm.fsm_fetchUserStart();
      }
    },

    fsm_fetchUserEnd: "FETCH_END",
    fsm_init: "INIT",
    fsm_reset: "RESET",
    fsm_error: "ERROR",
    fsm_exit: "EXIT",
    fsm_fetchUserStart: "FETCH_START",
  },

  FETCH_START: {
    onEnter: async function (event, prevState, newState) {
      if (event !== "fsm_tick") {
        reporter(event, prevState, newState);

        try {
          if (lastUserFetched === "RESTART") {
            console.log(chalkGreen(`${PF} | FETCH USER | RESTART`));
            lastUserFetched = configuration.testMode
              ? parseInt(10000000 * Math.random()).toString()
              : false;
          } else if (!lastUserFetched) {
            console.log(
              chalkGreen(
                `${PF} | LOAD LAST USER FETCHED | ${configHostFolder}/${lastUserFetchedFile}`
              )
            );

            const lastUserFetchedObj = await tcUtils.loadFile({
              folder: configHostFolder,
              file: lastUserFetchedFile,
              noErrorNotFound: true,
            });

            if (lastUserFetchedObj) {
              lastUserFetched = lastUserFetchedObj.nodeId.toString();
              if (!lastUserFetched) lastUserFetched = "1";
              console.log(
                chalkGreen(
                  `${PF} | LOADED LAST USER FETCHED | ${lastUserFetched}`
                )
              );
            } else {
              console.log(
                chalkError(
                  `${PF} | *** LAST USER FETCHED LOAD FILE NOT FOUND: ${configHostFolder}/${lastUserFetchedFile}`
                )
              );
            }
          }

          await initfetchUsers();
          fsm.fsm_fetchUser();
        } catch (err) {
          console.log(
            chalkError(`${PF} | *** INIT FETCH USER FRIEND ERROR: ${err}`)
          );
          fsm.fsm_error();
        }
      }
    },

    fsm_init: "INIT",
    fsm_reset: "RESET",
    fsm_error: "ERROR",
    fsm_fetchUser: "FETCH",
    fsm_fetchUserStart: "FETCH_START",
    fsm_exit: "EXIT",
    fsm_fetchUserEnd: "FETCH_END",
  },

  FETCH: {
    onEnter: async function (event, prevState, newState) {
      if (event !== "fsm_tick") {
        reporter(event, prevState, newState);
      }
    },

    fsm_init: "INIT",
    fsm_error: "ERROR",
    fsm_reset: "RESET",
    fsm_fetchUserContinue: "FETCH",
    fsm_exit: "EXIT",
    fsm_fetchUserEnd: "FETCH_END",
  },

  FETCH_END: {
    onEnter: function (event, prevState, newState) {
      if (event !== "fsm_tick") {
        reporter(event, prevState, newState);

        console.log(
          chalkBlueBold(`${PF} | +++ FETCH END +++ | PREV STATE: ${prevState}`)
        );

        if (configuration.quitOnComplete) {
          console.log(
            chalkBlueBold(
              `${PF} | +++ FETCH END +++ | QUIT ON COMPLETE | QUITTING ...`
            )
          );
          clearInterval(fetchUserInterval);
          fsm.fsm_exit();
        } else {
          console.log(chalkBlueBold(`${PF} | +++ FETCH END +++`));
          clearInterval(fetchUserInterval);

          const restartDelay = configuration.testMode
            ? 15 * ONE_SECOND
            : configuration.restartDelay;

          console.log(
            chalkAlert(
              `${PF} | +++ FETCH RESTART IN ${msToTime(
                restartDelay
              )} AT ${moment()
                .add(restartDelay, "ms")
                .format(compactDateTimeFormat)}`
            )
          );

          if (configuration.testMode) {
            console.log(
              chalkAlert(
                `${PF} | TEST MODE | LAST USER FETCHED: ${lastUserFetched}`
              )
            );
          } else {
            lastUserFetched = "RESTART";
          }

          setTimeout(function () {
            console.log(chalkGreen(`${PF} | <<< FETCH RESTART >>>`));

            fsm.fsm_fetchUserStart();
          }, restartDelay);
        }
      }
    },

    fsm_fetchUserStart: "FETCH_START",
    fsm_rateLimitStart: "PAUSE_RATE_LIMIT",
    fsm_fetchUserEnd: "FETCH_END",
    fsm_init: "INIT",
    fsm_error: "ERROR",
    fsm_exit: "EXIT",
    fsm_reset: "RESET",
  },

  EXIT: {
    onEnter: function (event, prevState, newState) {
      if (event !== "fsm_tick") {
        reporter(event, prevState, newState);
        console.log(
          chalkBlueBold(`${PF} | +++ FETCH END +++ | PREV STATE: ${prevState}`)
        );
        quit();
      }
    },
  },

  ERROR: {
    onEnter: function (event, prevState, newState) {
      if (event !== "fsm_tick") {
        reporter(event, prevState, newState);
        console.log(
          chalkError(
            `${PF} | *** FETCH END ERROR *** | PREV STATE: ${prevState}`
          )
        );
        quit();
      }
    },
  },
};

const fsm = Stately.machine(fsmStates);

function fsmStart(p) {
  return new Promise(function (resolve) {
    const params = p || {};
    const interval = params.fsmTickInterval || configuration.fsmTickInterval;

    console.log(
      chalkLog(PF + " | FSM START | TICK INTERVAL | " + msToTime(interval))
    );

    clearInterval(fsmTickInterval);

    fsm.fsm_tick();

    fsmTickInterval = setInterval(function () {
      fsm.fsm_tick();
    }, interval);

    fsm.fsm_init();

    resolve();
  });
}

reporter("START", "---", fsm.getMachineState());

console.log(PF + " | =================================");
console.log(PF + " | PROCESS TITLE: " + process.title);
console.log(PF + " | HOST:          " + hostname);
console.log(PF + " | PROCESS ID:    " + process.pid);
console.log(PF + " | RUN ID:        " + statsObj.runId);
console.log(
  PF +
    " | PROCESS ARGS   " +
    util.inspect(process.argv, { showHidden: false, depth: 1 })
);
console.log(PF + " | =================================");

console.log(
  chalkBlueBold(
    "\n=======================================================================\n" +
      PF +
      " | " +
      MODULE_ID +
      " STARTED | " +
      getTimeStamp() +
      "\n=======================================================================\n"
  )
);

setTimeout(async function () {
  try {
    const cnf = await initConfig(configuration);
    configuration = deepcopy(cnf);

    statsObj.status = "START";

    global.dbConnection = await mgUtils.connectDb({
      config: { autoReconnect: true },
    });

    initSaveFileQueue(configuration);

    if (configuration.testMode) {
      console.log(chalkAlert(PF + " | TEST MODE"));
    }

    console.log(
      chalkBlueBold(
        "\n--------------------------------------------------------" +
          "\n" +
          PF +
          " | " +
          configuration.processName +
          "\nCONFIGURATION\n" +
          jsonPrint(configuration) +
          "--------------------------------------------------------"
      )
    );

    if (!global.dbConnection) {
      await tcUtils.waitEvent("MONGO_DB_CONNECTED");
    }

    await fsmStart();
  } catch (err) {
    console.log(
      chalkError(PF + " | **** INIT CONFIG ERROR *****\n" + jsonPrint(err))
    );
    if (err.code !== 404) {
      quit({ cause: new Error("INIT CONFIG ERROR") });
    }
  }
}, 1000);
