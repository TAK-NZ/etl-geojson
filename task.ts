import hash from 'object-hash';
import { Feature } from '@tak-ps/node-cot';
import { Static, Type, TSchema } from '@sinclair/typebox';
import ETL, { Event, SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';

const Environment = Type.Object({
    URL: Type.String(),
    QueryParams: Type.Optional(Type.Array(Type.Object({
        key: Type.String(),
        value: Type.String()
    }))),
    Headers: Type.Optional(Type.Array(Type.Object({
        key: Type.String(),
        value: Type.String()
    }))),
    RemoveID: Type.Boolean({
        default: false,
        description: 'Remove the provided ID falling back to an Object Hash or Style Override'
    }),
    Timeout: Type.Number({
        default: 30000,
        description: 'Request timeout in milliseconds'
    }),
    Retries: Type.Number({
        default: 2,
        description: 'Number of retry attempts on failure'
    })
});

type SupportedGeometry = { type: 'Point'; coordinates: number[] } | { type: 'LineString'; coordinates: number[][] } | { type: 'Polygon'; coordinates: number[][][] };

export default class Task extends ETL {
    static name = 'etl-geojson';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    private createGeometry(type: string, coordinates: number[] | number[][] | number[][][]): SupportedGeometry | null {
        if (type === 'Point') {
            return { type: 'Point', coordinates: coordinates as number[] };
        } else if (type === 'LineString') {
            return { type: 'LineString', coordinates: coordinates as number[][] };
        } else if (type === 'Polygon') {
            return { type: 'Polygon', coordinates: coordinates as number[][][] };
        }
        return null;
    }

    private async fetchWithRetry(url: URL, headers: Record<string, string>, timeout: number, retries: number): Promise<Response> {
        for (let attempt = 0; attempt <= retries; attempt++) {
            try {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), timeout);
                
                const res = await fetch(url, {
                    method: 'GET',
                    headers,
                    signal: controller.signal
                });
                
                clearTimeout(timeoutId);
                
                if (!res.ok) {
                    throw new Error(`HTTP ${res.status}: ${res.statusText}`);
                }
                
                return res;
            } catch (error) {
                if (attempt === retries) throw error;
                console.log(`Attempt ${attempt + 1} failed, retrying...`);
                await new Promise(resolve => setTimeout(resolve, 1000 * (attempt + 1)));
            }
        }
        throw new Error('All retry attempts failed');
    }

    private processGeometryCollection(geometries: Array<{ type: string; coordinates: number[] | number[][] | number[][][] }>, baseId: string, properties: Record<string, unknown>): Array<{ id: string; type: 'Feature'; properties: { metadata: Record<string, unknown> }; geometry: SupportedGeometry }> {
        const features: Array<{ id: string; type: 'Feature'; properties: { metadata: Record<string, unknown> }; geometry: SupportedGeometry }> = [];
        
        geometries.forEach((geom, idx) => {
            const geometry = this.createGeometry(geom.type, geom.coordinates);
            if (geometry) {
                features.push({
                    id: `${baseId}-gc-${idx}`,
                    type: 'Feature',
                    properties: { metadata: properties },
                    geometry
                });
            } else {
                console.log(`Unsupported geometry type in GeometryCollection: ${geom.type}`);
            }
        });
        
        return features;
    }

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return Environment
            } else {
                return Type.Object({})
            }
        } else {
            return Type.Object({})
        }
    }

    async control(): Promise<void> {
        const env = await this.env(Environment);

        const url = new URL(env.URL);
        for (const param of env.QueryParams || []) {
            url.searchParams.append(param.key, param.value);
        }

        const headers: Record<string, string> = {};
        for (const header of env.Headers || []) {
            headers[header.key] = header.value;
        }

        const res = await this.fetchWithRetry(url, headers, env.Timeout, env.Retries);
        
        let body: { type: string; features: Array<{ id?: string; geometry?: { type: string; coordinates?: number[] | number[][] | number[][][]; geometries?: Array<{ type: string; coordinates: number[] | number[][] | number[][][] }> }; properties: Record<string, unknown> }> };
        try {
            body = await res.json();
        } catch (error) {
            throw new Error(`Invalid JSON response: ${error}`);
        }

        if (body.type !== 'FeatureCollection') {
            throw new Error('Only FeatureCollection is supported');
        }

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        };

        for (const feat of body.features) {
            if (env.RemoveID) delete feat.id;
            if (!feat.geometry) continue;

            const baseId = feat.id || hash(feat);

            if (feat.geometry.type === 'GeometryCollection' && feat.geometry.geometries) {
                const collectionFeatures = this.processGeometryCollection(feat.geometry.geometries, baseId, feat.properties);
                fc.features.push(...collectionFeatures);
            } else if (feat.geometry.type.startsWith('Multi') && feat.geometry.coordinates) {
                (feat.geometry.coordinates as (number[] | number[][] | number[][][])[]).forEach((coords, idx: number) => {
                    const geometryType = feat.geometry.type.replace('Multi', '');
                    const geometry = this.createGeometry(geometryType, coords);
                    if (geometry) {
                        fc.features.push({
                            id: `${baseId}-${idx}`,
                            type: 'Feature',
                            properties: { metadata: feat.properties },
                            geometry
                        });
                    }
                });
            } else if (feat.geometry.coordinates) {
                const geometry = this.createGeometry(feat.geometry.type, feat.geometry.coordinates);
                if (geometry) {
                    fc.features.push({
                        id: baseId,
                        type: 'Feature',
                        properties: { metadata: feat.properties },
                        geometry
                    });
                } else {
                    console.log(`Unsupported geometry type: ${feat.geometry.type}`);
                }
            }
        }

        console.log(`ok - obtained ${fc.features.length} features`);
        await this.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(await Task.init(import.meta.url), event);
}

