/*!
 * MIT License
 *
 * Copyright (c) 2024 デジタル庁
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
import { ICommonDbGeocode } from '@interface/database/common-db';
import { Readable, Writable } from "node:stream";
import { Query } from '@usecases/geocode/models/query';
import { ChomeTranform } from '@usecases/geocode/steps/chome-transform';
import { CityAndWardTransform } from '@usecases/geocode/steps/city-and-ward-transform';
import { CountyAndCityTransform } from '@usecases/geocode/steps/county-and-city-transform';
import { GeocodeResultTransform } from '@usecases/geocode/steps/geocode-result-transform';
import { KoazaTransform } from '@usecases/geocode/steps/koaza-transform';
import { NormalizeTransform } from '@usecases/geocode/steps/normalize-transform';
import { OazaChomeTransform } from '@usecases/geocode/steps/oaza-chome-transform';
import { ParcelTransform } from '@usecases/geocode/steps/parcel-transform';
import { PrefTransform } from '@usecases/geocode/steps/pref-transform';
import { RegExTransform } from '@usecases/geocode/steps/regex-transform';
import { RsdtBlkTransform } from '@usecases/geocode/steps/rsdt-blk-transform';
import { RsdtDspTransform } from '@usecases/geocode/steps/rsdt-dsp-transform';
import { Tokyo23TownTranform } from '@usecases/geocode/steps/tokyo23town-transform';
import { Tokyo23WardTranform } from '@usecases/geocode/steps/tokyo23ward-transform';
import { WardAndOazaTransform } from '@usecases/geocode/steps/ward-and-oaza-transform';
import { WardTransform } from '@usecases/geocode/steps/ward-transform';
import {DebugWorkerLogger} from "@domain/services/logger/debug-worker-logger";
import {GeocodeWorkerD1Controller} from "@interface/database/D1/geocode-worker-d1-controller";
import {AbrGeocoderInput} from "@usecases/geocode/models/abrg-input-data";


export const HonoWorkerAbrGeocoder = async ({
  dbCtrl,
  logger,
  callbackQuery,
  data
                                            }: {
  dbCtrl: GeocodeWorkerD1Controller;
  logger?: DebugWorkerLogger | undefined,
  callbackQuery: (query: Query) => void;
  data: AbrGeocoderInput;
}) => {
  const commonDb: ICommonDbGeocode = await dbCtrl.openCommonDb();

  const start = Date.now();
  const prefList = await commonDb.getPrefList();

  const prefTransform = new PrefTransform({
    prefList,
    logger,
  });

  const countyAndCityList = await commonDb.getCountyAndCityList();
  const countyAndCityTransform = new CountyAndCityTransform({
    countyAndCityList,
    logger,
  });
  console.log("test")

  const cityAndWardList = await commonDb.getCityAndWardList();
  const cityAndWardTransform = new CityAndWardTransform({
    cityAndWardList,
    logger,
  });

  const wardAndOazaList = await commonDb.getWardAndOazaChoList();
  const wardAndOazaTransform = new WardAndOazaTransform({
    wardAndOazaList,
    logger,
  });

  const wardList = await commonDb.getWards();
  const wardTransform = new WardTransform({
    db: commonDb,
    wards: wardList,
    logger,
  });

  const tokyo23towns = await commonDb.getTokyo23Towns();
  const tokyo23TownTransform = new Tokyo23TownTranform({
    tokyo23towns,
    logger,
  });

  const tokyo23wards = await commonDb.getTokyo23Wards();
  const tokyo23WardTransform = new Tokyo23WardTranform({
    tokyo23wards,
    logger,
  });

  const oazaChomes = await commonDb.getOazaChomes();

  const oazaChomeTransform = new OazaChomeTransform({
    oazaChomes,
    logger,
  });




  /*
  const [
    prefTransform,
    countyAndCityTransform,
    cityAndWardTransform,
    wardAndOazaTransform,
    oazaChomeTransform,
    tokyo23TownTransform,
    tokyo23WardTransform,
    wardTransform,
  ]: [
    PrefTransform,
    CountyAndCityTransform,
    CityAndWardTransform,
    WardAndOazaTransform,
    OazaChomeTransform,
    Tokyo23TownTranform,
    Tokyo23WardTranform,
    WardTransform,
  ] = await Promise.all([
    // 都道府県を試す
    new Promise(async (resolve: (result: PrefTransform) => void) => {
      const prefList = await commonDb.getPrefList();
      resolve(new PrefTransform({
        prefList,
        logger,
      }))
    }),

    // 〇〇郡〇〇市を試す
    new Promise(async (resolve: (result: CountyAndCityTransform) => void) => {
      const countyAndCityList = await commonDb.getCountyAndCityList();
      resolve(new CountyAndCityTransform({
        countyAndCityList,
        logger,
      }));
    }),

    // 〇〇市〇〇区を試す
    new Promise(async (resolve: (result: CityAndWardTransform) => void) => {
      const cityAndWardList = await commonDb.getCityAndWardList();
      resolve(new CityAndWardTransform({
        cityAndWardList,
        logger,
      }));
    }),
    // 〇〇市 (〇〇郡が省略された場合）を試す
    new Promise(async (resolve: (result: WardAndOazaTransform) => void) => {
      const wardAndOazaList = await commonDb.getWardAndOazaChoList();
      resolve(new WardAndOazaTransform({
        wardAndOazaList,
        logger,
      }));
    }),
    // 大字を試す
    new Promise(async (resolve: (result: OazaChomeTransform) => void) => {
      const oazaChomes = await commonDb.getOazaChomes();
      resolve(new OazaChomeTransform({
        oazaChomes,
        logger,
      }));
    }),

    // 東京23区を試す
    // 〇〇区＋大字の組み合わせで探す
    new Promise(async (resolve: (result: Tokyo23TownTranform) => void) => {
      const tokyo23towns = await commonDb.getTokyo23Towns();
      resolve(new Tokyo23TownTranform({
        tokyo23towns,
        logger,
      }));
    }),

    // 東京23区を試す
    new Promise(async (resolve: (result: Tokyo23WardTranform) => void) => {
      const tokyo23wards = await commonDb.getTokyo23Wards();
      resolve(new Tokyo23WardTranform({
        tokyo23wards,
        logger,
      }));
    }),

    // 〇〇区で始まるパターン(東京23区以外)
    new Promise(async (resolve: (result: WardTransform) => void) => {
      const wards = await commonDb.getWards();
      resolve(new WardTransform({
        db: commonDb,
        wards,
        logger,
      }));

    }),
  ]);
*/

  // 住所の正規化処理
  //
  // 例：
  //
  // 東京都千代田区紀尾井町1ー3 東京ガーデンテラス紀尾井町 19階、20階
  //  ↓
  // 東京都千代田区紀尾井町1{DASH}3{SPACE}東京ガーデンテラス紀尾井町{SPACE}19階、20階
  //
  const normalizeTransform = new NormalizeTransform({
    logger,
  });

  // 丁目を試す
  const chomeTransform = new ChomeTranform({
    db: commonDb,
    logger,
  });

  // 小字を試す
  const koazaTransform = new KoazaTransform({
    db: commonDb,
    logger,
  });

  // 地番の特定を試みる
  const parcelTransform = new ParcelTransform({
    dbCtrl,
    logger,
  });

  // 街区符号の特定を試みる
  const rsdtBlkTransform = new RsdtBlkTransform({
    dbCtrl,
    logger,
  });

  // 住居番号の特定を試みる
  const rsdtDspTransform = new RsdtDspTransform({
    dbCtrl,
    logger,
  });

  // 正規表現での特定を試みる
  const regexTranfrorm = new RegExTransform({
    logger,
  });

  // 最終的な結果にまとめる
  const geocodeResultTransform = new GeocodeResultTransform();

  const reader = new Readable({
    objectMode: true,
    read() {},
  });

  // メインスレッドに結果を送信する
  const dst = new Writable({
    objectMode: true,
    write(query: Query, _, callback) {
      callbackQuery(query);
      callback();
    },
  });


  logger?.info(`init: ${Date.now() - start}`);

  reader.pipe(normalizeTransform)
    .pipe(prefTransform)
    .pipe(countyAndCityTransform)
    .pipe(cityAndWardTransform)
    .pipe(wardAndOazaTransform)
    .pipe(wardTransform)
    .pipe(tokyo23TownTransform)
    .pipe(tokyo23WardTransform)
    .pipe(oazaChomeTransform)
    .pipe(chomeTransform)
    .pipe(koazaTransform)
    .pipe(rsdtBlkTransform)
    .pipe(rsdtDspTransform)
    .pipe(parcelTransform)
    .pipe(regexTranfrorm)
    .pipe(geocodeResultTransform)
    .pipe(dst);

  reader.push(data);
};
