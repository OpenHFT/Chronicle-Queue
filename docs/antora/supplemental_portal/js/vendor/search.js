/* eslint-env browser */
window.antoraLunr = (function (lunr) {
  const scriptAttrs = document.getElementById('search-script').dataset
  const basePath = scriptAttrs.basePath
  const pagePath = scriptAttrs.pagePath
  var searchInput = document.getElementById('search-input')
  var searchResult = document.createElement('div')
  searchResult.classList.add('search-result-dropdown-menu')
  searchInput.parentNode.appendChild(searchResult)

  function highlightText (doc, position) {
    var hits = []
    var start = position[0]
    var length = position[1]

    var text = doc.text
    var highlightSpan = document.createElement('span')
    highlightSpan.classList.add('search-result-highlight')
    highlightSpan.innerText = text.substr(start, length)

    var end = start + length
    var textEnd = text.length - 1
    var contextOffset = 40
    var contextAfter = end + contextOffset > textEnd ? textEnd : end + contextOffset
    var contextBefore = start - contextOffset < 0 ? 0 : start - contextOffset
    if (start === 0 && end === textEnd) {
      hits.push(highlightSpan)
    } else if (start === 0) {
      hits.push(highlightSpan)
      hits.push(document.createTextNode(text.substr(end, contextAfter)))
    } else if (end === textEnd) {
      hits.push(document.createTextNode(text.substr(0, start)))
      hits.push(highlightSpan)
    } else {
      hits.push(document.createTextNode('...' + text.substr(contextBefore, start - contextBefore)))
      hits.push(highlightSpan)
      hits.push(document.createTextNode(text.substr(end, contextAfter - end) + '...'))
    }
    return hits
  }

  function highlightTitle (hash, doc, position) {
    var hits = []
    var start = position[0]
    var length = position[1]

    var highlightSpan = document.createElement('span')
    highlightSpan.classList.add('search-result-highlight')
    var title
    if (hash) {
      title = doc.titles.filter(function (item) {
        return item.id === hash
      })[0].text
    } else {
      title = doc.title
    }
    highlightSpan.innerText = title.substr(start, length)

    var end = start + length
    var titleEnd = title.length - 1
    if (start === 0 && end === titleEnd) {
      hits.push(highlightSpan)
    } else if (start === 0) {
      hits.push(highlightSpan)
      hits.push(document.createTextNode(title.substr(length, titleEnd)))
    } else if (end === titleEnd) {
      hits.push(document.createTextNode(title.substr(0, start)))
      hits.push(highlightSpan)
    } else {
      hits.push(document.createTextNode(title.substr(0, start)))
      hits.push(highlightSpan)
      hits.push(document.createTextNode(title.substr(end, titleEnd)))
    }
    return hits
  }

  function highlightHit (metadata, hash, doc) {
    var hits = []
    for (var token in metadata) {
      var fields = metadata[token]
      for (var field in fields) {
        var positions = fields[field]
        if (positions.position) {
          var position = positions.position[0] // only higlight the first match
          if (field === 'title') {
            hits = highlightTitle(hash, doc, position)
          } else if (field === 'text') {
            hits = highlightText(doc, position)
          }
        }
      }
    }
    return hits
  }

  function createSearchResult(result, store, searchResultDataset) {
    result.forEach(function (item) {
      var url = item.ref
      var hash
      if (url.includes('#')) {
        hash = url.substring(url.indexOf('#') + 1)
        url = url.replace('#' + hash, '')
      }
      var doc = store[url]
      var metadata = item.matchData.metadata
      var hits = highlightHit(metadata, hash, doc)
      searchResultDataset.appendChild(createSearchResultItem(doc, item, hits))
    })
  }

  function createSearchResultItem (doc, item, hits) {
    var documentTitle = document.createElement('div')
    documentTitle.classList.add('search-result-document-title')
    documentTitle.innerText = doc.title
    var documentHit = document.createElement('div')
    documentHit.classList.add('search-result-document-hit')
    var documentHitLink = document.createElement('a')
    var rootPath = basePath
    documentHitLink.href = rootPath + item.ref
    documentHit.appendChild(documentHitLink)
    hits.forEach(function (hit) {
      documentHitLink.appendChild(hit)
    })
    var searchResultItem = document.createElement('div')
    searchResultItem.classList.add('search-result-item')
    searchResultItem.appendChild(documentTitle)
    searchResultItem.appendChild(documentHit)
    searchResultItem.addEventListener('mousedown', function (e) {
      e.preventDefault()
    })
    return searchResultItem
  }

  function createNoResult (text) {
    var searchResultItem = document.createElement('div')
    searchResultItem.classList.add('search-result-item')
    var documentHit = document.createElement('div')
    documentHit.classList.add('search-result-document-hit')
    var message = document.createElement('strong')
    message.innerText = 'No results found for query "' + text + '"'
    documentHit.appendChild(message)
    searchResultItem.appendChild(documentHit)
    return searchResultItem
  }

  function search (index, text) {
    // execute an exact match search
    var result = index.search(text)
    if (result.length > 0) {
      return result
    }
    // no result, use a begins with search
    result = index.search(text + '*')
    if (result.length > 0) {
      return result
    }
    // no result, use a contains search
    result = index.search('*' + text + '*')
    return result
  }

  function searchIndex (index, store, text) {
    // reset search result
    while (searchResult.firstChild) {
      searchResult.removeChild(searchResult.firstChild)
    }
    if (text.trim() === '') {
      return
    }
    var result = search(index, text)
    var searchResultDataset = document.createElement('div')
    searchResultDataset.classList.add('search-result-dataset')
    searchResult.appendChild(searchResultDataset)
    if (result.length > 0) {
      createSearchResult(result, store, searchResultDataset)
    } else {
      searchResultDataset.appendChild(createNoResult(text))
    }
  }

  function debounce (func, wait, immediate) {
    var timeout
    return function () {
      var context = this
      var args = arguments
      var later = function () {
        timeout = null
        if (!immediate) func.apply(context, args)
      }
      var callNow = immediate && !timeout
      clearTimeout(timeout)
      timeout = setTimeout(later, wait)
      if (callNow) func.apply(context, args)
    }
  }

  function init (data) {
    var index = Object.assign({index: lunr.Index.load(data.index), store: data.store})
    var search = debounce(function () {
      searchIndex(index.index, index.store, searchInput.value)
    }, 100)
    searchInput.addEventListener('keydown', search)

    // this is prevented in case of mousedown attached to SearchResultItem
    searchInput.addEventListener('blur', function (e) {
      while (searchResult.firstChild) {
        searchResult.removeChild(searchResult.firstChild)
      }
    })
  }

  return {
    init: init,
  }
})(window.lunr)
