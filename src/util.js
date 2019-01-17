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

exports.fancyIterator = (iterator) => {
  iterator[Symbol.asyncIterator] = function () { return this }
  return exports.ends(iterator)
}
