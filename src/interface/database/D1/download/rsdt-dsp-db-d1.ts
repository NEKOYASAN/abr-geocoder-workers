/*!
 * MIT License
 *
 * Copyright (c) 2024 デジタル庁
 * Copyright (c) 2024 NEKOYASAN
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
import { DataField } from "@config/data-field";
import { DbTableName } from "@config/db-table-name";
import { TableKeyProvider } from "@domain/services/table-key-provider";
import { IRsdtDspDbDownload } from "@interface/database/common-db";
import { Sqlite3Wrapper } from "@interface/database/sqlite3/better-sqlite3-wrap";
import { Statement } from "better-sqlite3";
import {D1Database} from "@cloudflare/workers-types";

export class RsdtDspDownloadD1 implements IRsdtDspDbDownload {
  private readonly d1Client: D1Database;

  constructor(d1Client: D1Database) {
    this.d1Client = d1Client;
  }

  async closeDb(): Promise<void> {
    // D1の場合はClose処理は不要
  }

  // Lat,Lonを テーブルにcsvのデータを溜め込む
  async rsdtDspPosCsvRows(rows: Record<string, string | number>[]): Promise<void> {
    const preparedStatement = this.d1Client.prepare(`
      INSERT INTO ${DbTableName.RSDT_DSP} (
        rsdtdsp_key,
        rsdtblk_key,
        ${DataField.REP_LAT.dbColumn},
        ${DataField.REP_LON.dbColumn}
      ) VALUES (
        ?1,
        ?2,
        ?3,
        ?4
      ) ON CONFLICT (rsdtdsp_key) DO UPDATE SET
        ${DataField.REP_LAT.dbColumn} = ?3,
        ${DataField.REP_LON.dbColumn} = ?4
      WHERE 
        rsdtdsp_key = ?1 AND
        (${DataField.REP_LAT.dbColumn} != ?3 OR 
        ${DataField.REP_LON.dbColumn} != ?4 OR 
        ${DataField.REP_LAT.dbColumn} IS NULL OR
        ${DataField.REP_LON.dbColumn} IS NULL)
    `);


    const lg_code = rows[0][DataField.LG_CODE.dbColumn].toString();

    const stmts = rows.map((row) => {
      row.rsdtblk_key = TableKeyProvider.getRsdtBlkKey({
        lg_code,
        machiaza_id: row[DataField.MACHIAZA_ID.dbColumn].toString(),
        blk_id: row[DataField.BLK_ID.dbColumn].toString(),
      });
      row.rsdtdsp_key = TableKeyProvider.getRsdtDspKey({
        lg_code,
        machiaza_id: row[DataField.MACHIAZA_ID.dbColumn] as string,
        blk_id: row[DataField.BLK_ID.dbColumn] as string,
        rsdt_id: row[DataField.RSDT_ID.dbColumn] as string,
        rsdt2_id: row[DataField.RSDT2_ID.dbColumn] as string,
        rsdt_addr_flg: row[DataField.RSDT_ADDR_FLG.dbColumn] as number,
      });
      return preparedStatement.bind(row.rsdtdsp_key, row.rsdtblk_key, row.rep_lat, row.rep_lon);
    });

    await this.d1Client.batch(stmts);
  }

  // テーブルにcsvのデータを溜め込む
  async rsdtDspCsvRows(rows: Record<string, string | number>[]) {
    const preparedStatement = this.d1Client.prepare(`
      INSERT INTO ${DbTableName.RSDT_DSP} (
        rsdtdsp_key,
        rsdtblk_key,
        ${DataField.RSDT_ID.dbColumn},
        ${DataField.RSDT2_ID.dbColumn},
        ${DataField.RSDT_NUM.dbColumn},
        ${DataField.RSDT_NUM2.dbColumn},
        ${DataField.RSDT_ADDR_FLG.dbColumn}
      ) VALUES (
        ?1,
        ?2,
        ?3,
        ?4,
        ?5,
        ?6,
        ?7
      ) ON CONFLICT (rsdtdsp_key) DO UPDATE SET
        ${DataField.RSDT_ID.dbColumn} = ?3,
        ${DataField.RSDT2_ID.dbColumn} = ?4,
        ${DataField.RSDT_NUM.dbColumn} = ?5,
        ${DataField.RSDT_NUM2.dbColumn} = ?6,
        ${DataField.RSDT_ADDR_FLG.dbColumn} = ?7
      WHERE 
        rsdtdsp_key = ?1
    `);



    const lg_code = rows[0][DataField.LG_CODE.dbColumn].toString();

    const stmts = rows.map((row) => {
      row.rsdtblk_key = TableKeyProvider.getRsdtBlkKey({
        lg_code,
        machiaza_id: row[DataField.MACHIAZA_ID.dbColumn].toString(),
        blk_id: row[DataField.BLK_ID.dbColumn].toString(),
      });
      row.rsdtdsp_key = TableKeyProvider.getRsdtDspKey({
        lg_code,
        machiaza_id: row[DataField.MACHIAZA_ID.dbColumn] as string,
        blk_id: row[DataField.BLK_ID.dbColumn] as string,
        rsdt_id: row[DataField.RSDT_ID.dbColumn] as string,
        rsdt2_id: row[DataField.RSDT2_ID.dbColumn] as string,
        rsdt_addr_flg: row[DataField.RSDT_ADDR_FLG.dbColumn] as number,
      });
      return preparedStatement.bind(row.rsdtdsp_key, row.rsdtblk_key, row.rsdt_id, row.rsdt2_id, row.rsdt_num, row.rsdt_num2, row.rsdt_addr_flg);
    });

    await this.d1Client.batch(stmts);
  }
}