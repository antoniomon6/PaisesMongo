/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Conector;

import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import java.util.ArrayList;
import javax.swing.JOptionPane;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 *
 * @author casaMesaRider
 */
public class Conector {

	private final String url = "mongodb://localhost:27017";
	private final String bd = "paisesDB";

	private MongoDatabase Mongodb;

	public Conector() {
	}

	public boolean conectar() {
		boolean b = false;

		MongoClient mongo = MongoClients.create(url);

		if (mongo != null) {
			Mongodb = mongo.getDatabase(bd);
			b = true;
		} else {
			JOptionPane.showMessageDialog(null, "Error de conexión con MongoDB", "Error", JOptionPane.ERROR_MESSAGE);
		}

		return b;
	}

	public boolean existeContinente(String nombrev) {
		MongoCollection<Document> col = Mongodb.getCollection("continentes");
		return col.find(new Document("nombre", nombrev)).first() != null;
	}

	public boolean anadirContinente(String nombrev) {
		boolean b = false;
		try {
			MongoCollection<Document> col = Mongodb.getCollection("continentes");
			col.insertOne(new Document("nombre", nombrev));
			b = true;
		} catch (MongoException e) {
			JOptionPane.showMessageDialog(null, "Error al escribir en Mongo: " + e.getMessage());
			b = false;
		}
		return b;
	}

	// Meto un String directamente el arrayList para simplificar ya que continente solo tiene nombre
	public ArrayList<String> obtenerContinentes() {
		ArrayList<String> lista = new ArrayList<>();
		MongoCollection<Document> col = Mongodb.getCollection("continentes");
		for (Document doc : col.find()) {
			lista.add(doc.getString("nombre"));
		}
		return lista;
	}

	// ================== OPERACIONES DE PAISES ==================
	public boolean existePais(String nombrev) {
		MongoCollection<Document> col = Mongodb.getCollection("paises");
		return col.find(new Document("nombre", nombrev)).first() != null;
	}

	public boolean anadirPais(String nombrev, int habitantesv, String nombreContinentev) {
		boolean b = false;

		if (nombrev != null && !nombrev.isEmpty()) {
			MongoCollection<Document> paisesCol = Mongodb.getCollection("paises");
			MongoCollection<Document> continentesCol = Mongodb.getCollection("continentes");

			// 1. Buscamos el documento del continente por su nombre
			Document continenteDoc = continentesCol.find(new Document("nombre", nombreContinentev)).first();

			if (continenteDoc != null) {
				// 2. Extraemos su ObjectId
				ObjectId continenteId = continenteDoc.getObjectId("_id");

				try {
					// 3. Creamos el país referenciando el ObjectId del continente
					Document pais = new Document("nombre", nombrev)
							.append("habitantes", habitantesv)
							.append("continenteId", continenteId);

					paisesCol.insertOne(pais);
					b = true;
				} catch (MongoWriteException e) {
					if (e.getCode() == 11000) {
						System.out.println("Error: duplicidad en la clave");
					} else {
						System.out.println("Error de escritura en Mongo: " + e.getMessage());
					}
				}
			} else {
				System.out.println("El continente no existe en la base de datos.");
			}
		} else {
			System.out.println("Nombre del país sin valor");
		}

		return b;
	}

	public boolean eliminarPais(String nombrev) {
		boolean b = false;
		try {
			MongoCollection<Document> col = Mongodb.getCollection("paises");
			Document doc = new Document("nombre", nombrev);
			if (doc != null) {
				col.findOneAndDelete(doc);
				b = true;
			}
		} catch (MongoException e) {
			JOptionPane.showMessageDialog(null, "Error al borrar en Mongo: " + e.getMessage());
			b = false;
		}
		return b;

	}

	public ArrayList<Document> obtenerPaises() {
		ArrayList<Document> lista = new ArrayList<>();
		MongoCollection<Document> col = Mongodb.getCollection("paises");
		for (Document doc : col.find()) {
			lista.add(doc);
		}
		return lista;
	}

	public ArrayList<Document> obtenerPaisesPorContinente(String nombreContinente) {
		ArrayList<Document> lista = new ArrayList<>();

		MongoCollection<Document> continentesCol = Mongodb.getCollection("continentes");
		// Buscamos el continente para obtener su _id
		Document continenteDoc = continentesCol.find(new Document("nombre", nombreContinente)).first();

		if (continenteDoc != null) {
			ObjectId continenteId = continenteDoc.getObjectId("_id");

			MongoCollection<Document> paisesCol = Mongodb.getCollection("paises");
			// Buscamos los países que tengan ese ObjectId 
			FindIterable<Document> docs = paisesCol.find(new Document("continenteId", continenteId));

			for (Document doc : docs) {
				lista.add(doc);
			}
			// Ordenamos la lista usando un Comparator
			lista.sort((Document d1, Document d2) -> {
				String nombre1 = d1.getString("nombre");
				String nombre2 = d2.getString("nombre");
				return nombre1.compareToIgnoreCase(nombre2);
			});
		}

		return lista;

	}

}
