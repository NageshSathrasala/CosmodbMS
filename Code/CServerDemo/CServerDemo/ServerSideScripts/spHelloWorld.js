function spHelloWorld() {
    var context = getContext();
    var response = context.getResponse();

    response.setBody('This is code is executed on CosmosDB Server side !')
}