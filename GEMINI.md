# HA ANWB Energie Integration

## Development Workflow

### Complete Deployment Protocol
When deploying bug fixes or features to the live Home Assistant instance, adhere to this robust protocol to ensure the changes are correctly pulled, loaded, and verified:
1.  **Commit and Push:** Commit your verified local changes and push them to the branch.
2.  **Pull via HACS:** Instruct the live Home Assistant instance to pull the latest branch via the HACS MCP tool using the specific repository ID for this plugin. Use ha_hacs_search to find the repository ID if unknown.
3.  **Restart Home Assistant:** Restart the core to force it to load the newly downloaded python files.
    ha_restart(confirm=True)
4.  **Wait for Boot:** Wait for the Home Assistant web interface to become responsive.
5.  **Allow Integration Setup:** Pause for an additional 10-15 seconds to give the integration time to authenticate and perform its initial data poll.
6.  **Verify State:** Query the live states of critical entities using the HA MCP tools to ensure the new code is executing correctly and not throwing exceptions or reporting as unavailable.
