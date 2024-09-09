import {Hono} from 'hono'
import {AbrGeocoderDiContainer} from "@usecases/geocode/models/abr-geocoder-di-container";
import {AbrGeocoder} from "@usecases/geocode/abr-geocoder";
import {SearchTarget} from "@domain/types/search-target";
import {Query} from "@usecases/geocode/models/query";
import {OutputFormat} from "@domain/types/output-format";
import {cors} from 'hono/cors'
import {poweredBy} from 'hono/powered-by'
import {D1Database} from "@cloudflare/workers-types";
import {GeocodeWorkerD1Controller} from "@interface/database/D1/geocode-worker-d1-controller";
import {HonoWorkerAbrGeocoder} from "@interface/hono-server/HonoWorkerAbrGeocoder";

export const app = new Hono<{
  Bindings: {
    DB: D1Database
  }
}>()

let geocoder: AbrGeocoder;

app.use(cors())
app.use(poweredBy())

const isSearchTarget = (target: string): target is SearchTarget => {
  return Object.values(SearchTarget).includes(target as SearchTarget);
}

const isFormat = (format: string): format is OutputFormat => {
  return Object.values(OutputFormat).includes(format as OutputFormat);
}

app.get('/geocode', async (c) => {

  const {address, target = SearchTarget.ALL, fuzzy, format = "json"} = c.req.query();
  if (!address) {
    return c.json({
      status: 'error',
      message: 'The address paramaeter is empty',
    }, {
      status: 400,
      statusText: "address is empty"
    });
  }
  if (!isSearchTarget(target)) {
    return c.json({
      status: 'error',
      message: 'The target paramaeter is invalid',
    }, {
      status: 400,
      statusText: "target is invalid"
    });
  }
  if (fuzzy && fuzzy.length !== 1) {
    return c.json({
      status: 'error',
      message: 'The fuzzy paramaeter is invalid',
    }, {
      status: 400,
      statusText: "fuzzy is invalid"
    });
  }

  if (!isFormat(format)) {
    return c.json({
      status: 'error',
      message: 'The format paramaeter is invalid',
    }, {
      status: 400,
      statusText: "format is invalid"
    });
  }

  const databaseController = new GeocodeWorkerD1Controller({
    connectParams: {
      type: "d1",
      d1Client: c.env.DB,
    }
  });

  const queryResult = await new Promise(async (resolve: (query: Query) => void) => {
    await HonoWorkerAbrGeocoder({
      dbCtrl: databaseController,
      callbackQuery: resolve,
      logger: undefined,
      data: {
        address,
        tag: undefined,
        searchTarget: target,
        fuzzy,
      }
    })
  })


  return c.json({
    query: {
      input: queryResult.input.data.address,
    },
    result: {
      output: queryResult.formatted.address,
      other: queryResult.tempAddress?.toOriginalString() || "",
      score: queryResult.formatted.score,
      match_level: queryResult.match_level.str,
      coordinate_level: queryResult.coordinate_level.str,
      lat: queryResult.rep_lat,
      lon: queryResult.rep_lon,
      lg_code: queryResult.lg_code ? queryResult.lg_code : "",
      machiaza_id: queryResult.machiaza_id || "",
      rsdt_addr_flg: queryResult.rsdt_addr_flg,
      blk_id: queryResult.block_id || "",
    }
  }, {
    status: 200,
    statusText: "Success"
  })
})

export default app
