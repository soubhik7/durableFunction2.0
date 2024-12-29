namespace Durable.Function.TxDataPatch.Validation.Helper;

public class TxDataTracker
{
    public Customertechnicalheader customerTechnicalHeader { get; set; }
}

public class Customertechnicalheader
{
    public string correlationId { get; set; }
}