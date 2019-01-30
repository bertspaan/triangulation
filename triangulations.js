const fs = require('fs')
const path = require('path')
const csv = require('csv-parser')
const H = require('highland')
const R = require('ramda')
const wkx = require('wkx')

const geojson = {
  open: '{"type":"FeatureCollection","features":[',
  close: ']}\n',
  separator: ',\n'
}

function toFeature (object) {
  if (object.geometry) {
    return {
      type: 'Feature',
      properties: R.omit(['geometry'], object),
      geometry: object.geometry
    }
  }
}

function wkbStringToPoint (wkbString) {
  const intArray = wkbString.match(/.{2}/g)
    .map((hex) => parseInt(hex, 16))
  const point = wkx.Geometry.parse(Buffer.from(intArray))

  return {
    type: 'Point',
    coordinates: [point.x, point.y]
  }
}

function writeGeoJSON (filename, rows) {
  H([
    H([geojson.open]),
    rows
      .map(toFeature)
      .map(JSON.stringify)
      .intersperse(',\n'),
    H([geojson.close])
  ]).compact()
    .sequence()
    .pipe(fs.createWriteStream(filename, 'utf8'))
}

// ==================================================================
// 1815
// ==================================================================

const rows1815 = H(fs.createReadStream('krayenhof_1815.csv'))
  .pipe(csv({
    separator: '\t',
  }))

const toAmersfoort = 2.338
// 0.001100000000000989

const objects1815 = H(rows1815)
  .map((row) => ({
    standplaats: row.standplaats,
    // nnr: '8200',
    // onr: '8695',
    // gid: '59',
    // nr: '52',
    // volgnr: '24'
    geometry: {
      type: 'Point',
      coordinates: [
        parseInt(row.ograad) + parseInt(row.ominuut) / 60 + parseInt(row.oseconde) / 60 / 60 + toAmersfoort,
        parseInt(row.ngraad) + parseInt(row.nminuut) / 60 + parseInt(row.nseconde) / 60 / 60
      ]
    }
  }))

writeGeoJSON('krayenhof-1815.geojson', objects1815)

// ==================================================================
// 1850
// ==================================================================

const rows1850 = H(fs.createReadStream('triang1850.csv'))
  .pipe(csv({
    separator: '\t',
  }))

const objects1850 = H(rows1850)
  .map((row) => ({
    standplaats: row.standplaats,
    pagina: parseInt(row.pagina),
    volgnummer: parseInt(row.volgnr),
    // x: '71241',
    // y: '-73823',
    // rang: '2',
    // latlon_bonne: '01010000206E6C04009FD62562C9941740E9004CCA7B6A4940',
    // crd_bonne: '01010000206C6C0400000000009064F14000000000F005F2C0' }
    geometry: {
      type: 'Point',
      coordinates: [
        parseFloat(row.ol),
        parseFloat(row.nb)
      ]
    }
  }))

writeGeoJSON('1850.geojson', objects1850)

// ==================================================================
// 1929
// ==================================================================

const rows1929 = H(fs.createReadStream('triang1929.csv'))
  .pipe(csv({
    separator: '\t',
    quote: '|',
  }))

const objects1929 = H(rows1929)
  .map((row) => ({
    txt1: row.txt1,
    txt2: row.txt2,
    jaar: row.jaar,
    // gid: '108670',
    // page: '151',
    // nr: '3496',
    // pagenr: '151-3496',
    // crd_bessel: '0101000020806C0400CDCCCCCCF0D70041E17A14AE1DD31A41',
    // crd_wgs84: '010100002040710000CDCCCCCCF0D70041E17A14AE1DD31A41',
    // ast: '*',
    // excl: ' ',
    // crss: ' ',
    // latlon_bessel: '0101000020D41000005D366A26768F1440E25A4A1EE9F84940',
    // latlon_wgs84: '0101000020E61000001E6B46550D8F14406455608FC9F84940'
    geometry: wkbStringToPoint(row.latlon_wgs84)
  }))

writeGeoJSON('1929.geojson', objects1929)
