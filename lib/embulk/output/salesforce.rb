Embulk::JavaPlugin.register_output(
  "salesforce", "org.embulk.output.SalesforceOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
