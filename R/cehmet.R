library(git2r)

#' cehmet
#'
#' Clone a cehmet, change remote origin, push
#' @param cehmet url to the cehmet repository
#' @param dest destination remote url
#' @param public should the new GitHub repository be public or private?
#'
#' @param ... additional arguments to git2r functions
#'
#' @details url can be any git address we can use to clone (e.g. https://, git@@github.com:)
#' @importFrom git2r clone remote_add remote_remove push
#' @importFrom gh gh
#' @examples
#' \dontrun{
#' cehmet("https://github.com/NERC-CEH/cehmet",
#'          "https://github.com/peterlevy/mypkg")
#' }
cehmet <- function(cehmet, dest, public = FALSE, ...){
  repo <- git2r::clone(cehmet)
  git2r::remote_remove(repo, "origin")
  git2r::remote_add(repo, "origin", dest)

  ## Create repo via the GitHub API

  #git2r::push(repo)
}

