library(shiny)
library(ggplot2)

ui <- fluidPage(
  titlePanel("Prepayment Model"),
  
  sidebarLayout(
    sidebarPanel(),
    
    mainPanel(
      tabsetPanel(type = "tab",
                  tabPanel("Data", tableOutput("PCAtable")),
                  tabPanel("Plot", plotOutput("PCAplot"))
      )
    )
  )
)

server <- function(input, ouput){
  #output$PCAtable <- renderTable({head(iris)})
  output$PCAplot <- renderImage({
    # A temp file to save the output.
    # This file will be removed later by renderImage
    outfile <- tempfile(fileext='Var&Cum.png')
    
    # Generate the PNG
    png(outfile, width=700, height=500)
    hist(rnorm(input$obs), main="Generated in renderImage()")
    dev.off()
    
    # Return a list containing the filename
    list(src = outfile,
         contentType = 'image/png',
         width = 700,
         height = 500,
         alt = "This is alternate text")
  }, deleteFile = TRUE)
}

shinyApp(ui = ui, server = server)

