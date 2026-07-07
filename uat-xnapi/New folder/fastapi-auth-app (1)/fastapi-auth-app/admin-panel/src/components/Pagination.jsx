export default function Pagination({ page, perPage, total, onPage }) {
  const totalPages = Math.ceil(total / perPage)
  if (totalPages <= 1) return null

  const pages = []
  const delta = 2
  for (let i = Math.max(1, page - delta); i <= Math.min(totalPages, page + delta); i++) {
    pages.push(i)
  }

  return (
    <div className="flex items-center justify-between px-4 py-3 border-t border-gray-200">
      <p className="text-sm text-gray-500">
        Showing <span className="font-medium">{(page - 1) * perPage + 1}</span>
        {' – '}
        <span className="font-medium">{Math.min(page * perPage, total)}</span>
        {' of '}
        <span className="font-medium">{total}</span> users
      </p>

      <div className="flex items-center gap-1">
        <button
          onClick={() => onPage(page - 1)}
          disabled={page === 1}
          className="px-2 py-1 rounded text-sm text-gray-600 hover:bg-gray-100
                     disabled:opacity-40 disabled:cursor-not-allowed"
        >
          ‹
        </button>

        {pages[0] > 1 && (
          <>
            <button onClick={() => onPage(1)} className="px-3 py-1 rounded text-sm text-gray-600 hover:bg-gray-100">1</button>
            {pages[0] > 2 && <span className="text-gray-400 text-sm px-1">…</span>}
          </>
        )}

        {pages.map((p) => (
          <button
            key={p}
            onClick={() => onPage(p)}
            className={`px-3 py-1 rounded text-sm font-medium transition-colors ${
              p === page
                ? 'bg-brand-500 text-white'
                : 'text-gray-600 hover:bg-gray-100'
            }`}
          >
            {p}
          </button>
        ))}

        {pages[pages.length - 1] < totalPages && (
          <>
            {pages[pages.length - 1] < totalPages - 1 && <span className="text-gray-400 text-sm px-1">…</span>}
            <button onClick={() => onPage(totalPages)} className="px-3 py-1 rounded text-sm text-gray-600 hover:bg-gray-100">{totalPages}</button>
          </>
        )}

        <button
          onClick={() => onPage(page + 1)}
          disabled={page === totalPages}
          className="px-2 py-1 rounded text-sm text-gray-600 hover:bg-gray-100
                     disabled:opacity-40 disabled:cursor-not-allowed"
        >
          ›
        </button>
      </div>
    </div>
  )
}
