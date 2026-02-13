function! db#adapter#mysql#canonicalize(url) abort
  let url = substitute(a:url, '^mysql\d*:/\@!', 'mysql:///', '')
  " JDBC
  let url = substitute(url, '//address=(\(.*\))\(/[^#]*\)', '\="//".submatch(2)."&".substitute(submatch(1), ")(", "\\&", "g")', '')
  let url = substitute(url, '[&?]', '?', '')
  return db#url#absorb_params(url, {
        \ 'user': 'user',
        \ 'password': 'password',
        \ 'path': 'host',
        \ 'host': 'host',
        \ 'port': 'port'})
endfunction

function! s:command_for_url(url) abort
  let params = db#url#parse(a:url).params
  let command = ['mysql']
  for i in keys(params)
    let command += ['--'.i.'='.params[i]]
  endfor
  " -S only works for localhost, so force that, in case the default was overridden, e.g. in .my.cnf
  return command + db#url#as_argv(a:url, '-h ', '-P ', '-h localhost -S ', '-u ', '-p', '')
endfunction

function! db#adapter#mysql#interactive(url) abort
  return s:command_for_url(a:url)
endfunction

function! db#adapter#mysql#filter(url) abort
  return s:command_for_url(a:url) + ['-t', '--binary-as-hex']
endfunction

function! db#adapter#mysql#auth_pattern() abort
  return '^ERROR 104[45] '
endfunction

function! db#adapter#mysql#complete_opaque(url) abort
  return db#adapter#mysql#complete_database('mysql:///')
endfunction

function! db#adapter#mysql#complete_database(url) abort
  let pre = matchstr(a:url, '[^:]\+://.\{-\}/')
  let cmd = s:command_for_url(pre)
  let out = db#systemlist(cmd + ['-e', 'show databases'])
  return out[1:-1]
endfunction

function! db#adapter#mysql#tables(url) abort
  return db#systemlist(s:command_for_url(a:url) + ['-e', 'show tables'])[1:-1]
endfunction

function! s:mysql_schema(url) abort
  return substitute(db#url#parse(a:url).path, '^/', '', '')
endfunction

function! s:schema_expression(url) abort
  let schema = s:mysql_schema(a:url)
  if empty(schema)
    return 'DATABASE()'
  endif
  return "'".substitute(schema, "'", "''", 'g')."'"
endfunction

function! s:strip_warnings(lines) abort
  return filter(copy(a:lines), "v:val !~? '^mysql: \\[Warning\\]' && !empty(v:val)")
endfunction

function! s:first_column(url, sql) abort
  let out = s:strip_warnings(db#systemlist(s:command_for_url(a:url) + ['-e', a:sql]))
  if len(out) <= 1
    return []
  endif
  let result = []
  for line in out[1:]
    let cols = split(line, '\t')
    if !empty(cols) && !empty(cols[0])
      call add(result, cols[0])
    endif
  endfor
  return result
endfunction

" Add views function
function! db#adapter#mysql#views(url) abort
  let sql = 'SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '.s:schema_expression(a:url).' ORDER BY TABLE_NAME'
  return s:first_column(a:url, sql)
endfunction

" Add procedures function
function! db#adapter#mysql#procedures(url) abort
  let sql = "SELECT ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ".s:schema_expression(a:url)." AND ROUTINE_TYPE = 'PROCEDURE' ORDER BY ROUTINE_NAME"
  return s:first_column(a:url, sql)
endfunction

" Add functions function
function! db#adapter#mysql#functions(url) abort
  let sql = "SELECT ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ".s:schema_expression(a:url)." AND ROUTINE_TYPE = 'FUNCTION' ORDER BY ROUTINE_NAME"
  return s:first_column(a:url, sql)
endfunction

" Add events function
function! db#adapter#mysql#events(url) abort
  let sql = 'SELECT EVENT_NAME FROM information_schema.EVENTS WHERE EVENT_SCHEMA = '.s:schema_expression(a:url).' ORDER BY EVENT_NAME'
  return s:first_column(a:url, sql)
endfunction
