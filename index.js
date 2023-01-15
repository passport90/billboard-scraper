const fs = require('fs')
const { request } = require('undici')
const { DateTime, Duration } = require('luxon')
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
  if (rows.length !== 100) {
    console.error('Entries are not 100!', fileName)
    return
  }
  for (let i = 0; i < rows.length; ++i) {
    const row = rows[i]
    const title = row.querySelector('h3#title-of-a-story')?.firstChild.nodeValue.trim()
    const artist = row.querySelector('h3#title-of-a-story + span')?.firstChild.nodeValue.trim()
    if (title === null || artist === null) {
      console.error('Unexpected HTML!', position, title, artist)
      throw new Error('Unexpected HTML!')
    }
    yield { position: i + 1, artist, title }
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
      if (values[0] === 1984 && values[1] === 7 && values[4] === "Remember The Nights") {
        values[2] = "87"
      }

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
  if (process.argv.length < 4) {
    console.error('Missing year start and end!')
    console.info('Example: node index.js 1998 2020')
    return
  }

  const yearStart = parseInt(process.argv[2])
  const yearEnd = parseInt(process.argv[3])
  if (isNaN(yearStart) || isNaN(yearEnd)) {
    console.error('Date parse error!', yearStart, yearEnd)
    return
  }

  if (yearStart < 1958 || yearEnd > 2022) {
    console.error('Year outside available range of 1958-2022!', yearStart, yearEnd)
    return
  }

  for (let year = yearStart; year <= yearEnd; ++year) {
    let dateFrom
    if (year === 1958) {
      dateFrom = DateTime.fromISO(`${year}-W31-6T00:00Z`)
    } else {
      dateFrom = DateTime.fromISO(`${year}-W01-6T00:00Z`)
    }

    const dateTo = DateTime.fromISO(`${parseInt(year) + 1}-W01-6T00:00Z`)
    if (!(dateFrom.isValid && dateTo.isValid)) {
      console.error('Date parse error!', year)
      return
    }
    const start = DateTime.now()

    const jsonlFileName = `jsonl/${year}.jsonl`
    if (!fs.existsSync(jsonlFileName)) {
      for (let date = dateFrom; +date < +dateTo; date = date.plus({ weeks: 1 })) {
        if (date.toISOWeekDate() === '1961-W52-6') {
          continue
        }
        await ingest(date, jsonlFileName)
      }
    }

    try {
      await store(jsonlFileName)
    } catch (err) {
      throw err
      // If error raised, reparse
      console.error(err)
      console.info(`Reparsing ${year}...`)
      fs.unlinkSync(jsonlFileName)
      --year
      continue
    }
    console.info(`Time elapsed: ${Duration.fromMillis(+(DateTime.now() - +start)).toHuman()}.`)
  }
}

main().then(() => console.info('Done!')).catch(console.error)


