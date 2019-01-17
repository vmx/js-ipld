'use strict'

exports.first = async (iterator) => {
  for await (const value of iterator) {
    return value
  }
}

exports.last = async (iterator) => {
  let value
  for await (value of iterator) {
    // Intentionally empty
  }
  return value
}

exports.ends = (iterator) => {
  iterator.first = () => exports.first(iterator)
  iterator.last = () => exports.last(iterator)
  return iterator
}

exports.all = async (iterator) => {
  const values = []
  for await (const value of iterator) {
    values.push(value)
  }
  return values
}

exports.fancyIterator = (iterator) => {
  iterator[Symbol.asyncIterator] = function () { return this }
  iterator.all = () => exports.all(iterator)
  return exports.ends(iterator)
}
