/**
 * invoice_pdf_restlet.js
 *
 * SuiteScript 2.1 RESTlet — returns a base64-encoded PDF for a given invoice ID.
 *
 * Deploy as a RESTlet in NetSuite, then call via:
 *   GET https://<accountId>.restlets.api.netsuite.com/app/site/hosting/restlet.nl
 *       ?script=<scriptId>&deploy=1&invoiceId=<id>
 *
 * @NApiVersion 2.1
 * @NModuleScope SameAccount
 * @NScriptType RESTlet
 */
define(['N/render'], (render) => {

    const get = (params) => {
        const invoiceId = parseInt(params.invoiceId, 10);
        if (!invoiceId) {
            return { error: 'Missing invoiceId parameter' };
        }

        const pdfFile = render.transaction({
            entityId: invoiceId,
            printMode: render.PrintMode.PDF,
        });

        return {
            filename: `invoice_${invoiceId}.pdf`,
            content:  pdfFile.getContents(),
        };
    };

    return { get };
});
