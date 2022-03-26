const fs = require('fs')
const { request } = require('undici')
const { DateTime } = require('luxon')
const { JSDOM } = require('jsdom')
const { Client: PgClient } = require('pg')
const readline = require('readline')

const download = async (date, htmlFileName) => {
  console.info(`Downloading HTML for ${date.toISODate()}...`)
  const {
    statusCode,
    headers,
    body,
  } = await request(`https://www.billboard.com/charts/hot-100/${date.toISODate()}/`)

  if (statusCode !== 200) {
    console.error('Status code is not 200!', statusCode)
    return null
  }

  const contentTypeHeader = headers['content-type']
  if (contentTypeHeader !== 'text/html; charset=UTF-8') {
    console.error('Content type is unexpected!', contentTypeHeader)
    return null
  }

  const writeStream = fs.createWriteStream(htmlFileName)
  for await (const data of body) {
    writeStream.write(data)
  }
  writeStream.end()
}

async function* parseHtml(fileName) {
  console.info(`Parsing ${fileName}...`)
  const dom = await JSDOM.fromFile(fileName)
  const rows = dom.window.document.querySelectorAll('div.o-chart-results-list-row-container')
  for (const row of rows) {
    const position = row.querySelector('li.o-chart-results-list__item:first-child > span')?.firstChild.nodeValue.trim()
    const title = row.querySelector('h3#title-of-a-story')?.firstChild.nodeValue.trim()
    const artist = row.querySelector('h3#title-of-a-story + span')?.firstChild.nodeValue.trim()
    if (position === null || title === null || artist === null) {
      console.error('Unexpected HTML', position, title, artist)
      throw new Error('Unexpected HTML')
    }
    yield { position, artist, title }
  }
}

const ingest = async (date, jsonlFileName) => {
  const htmlFileName = `html/${date.toISODate()}.html`
  if (!fs.existsSync(htmlFileName)) {
    await download(date, htmlFileName)
  }

  const writeStream = fs.createWriteStream(jsonlFileName, { flags: 'a' })
  console.info(`Writing parsed HTML to ${jsonlFileName}...`)
  for await (const { position, artist, title } of parseHtml(htmlFileName)) {
    writeStream.write(JSON.stringify([date.weekYear, date.weekNumber, position, artist, title]))
    writeStream.write('\n')
  }
  writeStream.end()

  console.info(`Process done for ${date.toISODate()}.`)
}

const store = async jsonlFileName => {
  if (!fs.existsSync(jsonlFileName)) {
    console.error(`JSONL file ${jsonlFileName} does not exist!`)
    return
  }

  const pgClient = new PgClient()
  pgClient.connect()
  await pgClient.query('BEGIN')

  console.info(`Inserting data from ${jsonlFileName} to database...`)
  try {
    const rl = readline.createInterface(fs.createReadStream(jsonlFileName), { crlfDelay: Infinity })
    for await (const line of rl) {
      const values = JSON.parse(line)
      await pgClient.query(
        'INSERT INTO raw_chart (year, week, position, artist, title) VALUES ($1, $2, $3, $4, $5)',
        values,
      )
    }
    await pgClient.query('COMMIT')
  } catch (err) {
    await pgClient.query('ROLLBACK')
    throw err
  }

  pgClient.end()
}

const main = async () => {
  if (process.argv.length < 3) {
    console.error('Missing year!')
    console.info('Example: node index.js 2020')
    return
  }

  const yearInput = parseInt(process.argv[2])
  if (isNaN(yearInput)) {
    console.error('Date parse error!', yearInput)
    return
  }
  const dateFrom = DateTime.fromISO(`${yearInput}-W01-6T00:00Z`)
  const dateTo = DateTime.fromISO(`${parseInt(yearInput) + 1}-W01-6T00:00Z`)
  if (!(dateFrom.isValid && dateTo.isValid)) {
    console.error('Date parse error!', yearInput)
    return
  }

  const jsonlFileName = `jsonl/${yearInput}.jsonl`
  if (!fs.existsSync(jsonlFileName)) {
    for (let date = dateFrom; +date < +dateTo; date = date.plus({ weeks: 1 })) {
      await ingest(date, jsonlFileName)
    }
  }
  await store(jsonlFileName)
}

main().then(() => console.info('Done!')).catch(console.error)


