package meetup.beeno.mapping;

/**
 * Represents a full HBase column qualifier (family + qualifier) for a field
 * mapping.
 */
public class ColumnQualifier {
  private String family;
  private String qualifier;

  public ColumnQualifier(String family, String qualifier) {
    this.family = family;
    if (qualifier == null || qualifier.equals("*")) {
      this.qualifier = "";
    } else {
      this.qualifier = qualifier;
    }
  }

  public String getFamily() {
    return family;
  }

  public String getQualifier() {
    return qualifier;
  }

  public boolean matches(String family, String qualifier) {
    return this.family.equals(family) && this.qualifier.equals(qualifier);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof ColumnQualifier)) {
      return false;
    }

    ColumnQualifier otherQual = (ColumnQualifier)other;
    return matches(otherQual.getFamily(), otherQual.getQualifier());
  }

  @Override
  public int hashCode() {
    int result = family != null ? family.hashCode() : 0;
    result = 31 * result + (qualifier != null ? qualifier.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return family+":"+qualifier;
  }
}
