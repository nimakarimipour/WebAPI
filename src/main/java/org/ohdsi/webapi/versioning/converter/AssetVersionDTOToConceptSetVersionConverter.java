package org.ohdsi.webapi.versioning.converter;

import org.ohdsi.webapi.versioning.domain.ConceptSetVersion;
import org.springframework.stereotype.Component;

@Component
public class AssetVersionDTOToConceptSetVersionConverter extends BaseAssetVersionFullDTOToAssetVersionFullConverter<ConceptSetVersion> {
    @Override
    protected ConceptSetVersion createResultObject() {
        return new ConceptSetVersion();
    }
}
