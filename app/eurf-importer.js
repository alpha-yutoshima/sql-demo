const fs = require('node:fs');
const path = require('node:path');
const { Writable } = require('node:stream');
const csv = require('csv-parser');
const { pipeline } = require('node:stream/promises');

class EurfImporter extends Writable {
  tableName = '';
  conn = null;

  constructor(mysql, tableName) {
    super({
      objectMode: true,
    });
    this.tableName = tableName;
    this.conn = mysql;
  }

  async _write(chunk, _, callback) {
    if (chunk['支所等'] === '合計' || chunk['性別'] === '合計' || chunk['年齢'] === '総数') {
      callback();
      return;
    }
    await this.conn.query(`
      insert into ${this.tableName}
      (yyyymm,lg_code,kobe_code,area,branch,sex,age,value)
      values (?,?,?,?,?,?,?,?)
    `, [
      chunk['対象年月'],
      chunk['区CD'],
      chunk['行政区CD'],
      chunk['区名'],
      chunk['支所等'],
      chunk['性別'],
      chunk['年齢'],
      chunk['値']
    ]);

    callback();
  }

  static _createTable = async (conn, tableName) => {
    await conn.query(`DROP TABLE IF EXISTS ${tableName};`)
    await conn.query(`CREATE TABLE ${tableName} (
  eurf_id INT AUTO_INCREMENT PRIMARY KEY NOT NULL,
  yyyymm VARCHAR(6),
  lg_code VARCHAR(5),
  kobe_code VARCHAR(5),
  area VARCHAR(255),
  branch VARCHAR(255),
  sex VARCHAR(2),
  age INT,
  value INT
)`);
  }

  static fromCsv = async (params) => {
    const filename = params.dirent.name;
    const [year] = path.basename(filename).split(/[\-\.]/);
    const tableName = `eurf310005_${year}`;

    const reader = fs.createReadStream(`${params.dirent.parentPath}/${filename}`);
    const parser = csv();
    const csvImporter = new EurfImporter(params.conn, tableName);

    // テーブルの作成
    await EurfImporter._createTable(params.conn, tableName);

    // トランザクションの開始
    await params.conn.query('start transaction');

    await pipeline(
      // CSVファイルから読み込む
      reader,
      // CSVファイルのパース
      parser,
      // データベースに取り込む
      csvImporter,
    );

    // コミットする
    await params.conn.query('commit');
  }
}

module.exports = {
  EurfImporter,
};
