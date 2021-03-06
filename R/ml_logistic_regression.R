#' Spark ML -- Logistic Regression
#'
#' Perform logistic regression on a \code{spark_tbl}.
#'
#' @template roxlate-ml-regression
#'
#' @family Spark ML routines
#'
#' @export
ml_logistic_regression <- function(x,
                                   response,
                                   features,
                                   intercept = TRUE,
                                   alpha = 0,
                                   lambda = 0,
                                   ...)
{
  df <- spark_dataframe(x)
  sc <- spark_connection(df)
  
  response <- ensure_scalar_character(response)
  features <- as.character(features)
  intercept <- ensure_scalar_boolean(intercept)
  alpha <- ensure_scalar_double(alpha)
  lambda <- ensure_scalar_double(lambda)
  only_model <- ensure_scalar_boolean(list(...)$only_model, default = FALSE)

  envir <- new.env(parent = emptyenv())
  tdf <- ml_prepare_dataframe(df, features, response, envir = envir)

  lr <- invoke_new(
    sc,
    "org.apache.spark.ml.classification.LogisticRegression"
  )

  model <- lr %>%
    invoke("setMaxIter", 100L) %>%
    invoke("setFeaturesCol", envir$features) %>%
    invoke("setLabelCol", envir$response) %>%
    invoke("setFitIntercept", as.logical(intercept)) %>%
    invoke("setElasticNetParam", as.double(alpha)) %>%
    invoke("setRegParam", as.double(lambda))
  
  if (only_model) return(model)  
  
  fit <- model %>%
    invoke("fit", tdf)

  coefficients <- fit %>%
    invoke("coefficients") %>%
    invoke("toArray")
  names(coefficients) <- features

  hasIntercept <- invoke(fit, "getFitIntercept")
  if (hasIntercept) {
    intercept <- invoke(fit, "intercept")
    coefficients <- c(coefficients, intercept)
    names(coefficients) <- c(features, "(Intercept)")
  }

  summary <- invoke(fit, "summary")
  areaUnderROC <- invoke(summary, "areaUnderROC")
  roc <- spark_dataframe_collect(invoke(summary, "roc"))
  
  coefficients <- intercept_first(coefficients)

  ml_model("logistic_regression", fit,
           features = features,
           response = response,
           coefficients = coefficients,
           roc = roc,
           area.under.roc = areaUnderROC,
           model.parameters = as.list(envir)
  )
}

#' @export
print.ml_model_logistic_regression <- function(x, ...) {

  # report what model was fitted
  formula <- paste(x$response, "~", paste(x$features, collapse = " + "))
  cat("Call: ", formula, "\n\n", sep = "")

  # report coefficients
  cat("Coefficients:", sep = "\n")
  print(x$coefficients)
}

#' @export
summary.ml_model_logistic_regression <- function(object, ...) {
  ml_model_print_call(object)
  print_newline()
  # ml_model_print_residuals(object)
  # print_newline()
  ml_model_print_coefficients(object)
  print_newline()
}


#' @export
residuals.ml_model_logistic_regression <- function(object, ...) {
  stop("residuals not yet available for Spark logistic regression")
}

